package controller

import (
	"context"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"

	dolphinv1alpha1 "github.com/zncdatadev/dolphinscheduler-operator/api/v1alpha1"
	"github.com/zncdatadev/dolphinscheduler-operator/internal/security"
	"github.com/zncdatadev/dolphinscheduler-operator/internal/util/version"
	"github.com/zncdatadev/dolphinscheduler-operator/pkg/constant"
	"github.com/zncdatadev/dolphinscheduler-operator/pkg/util"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opgoconstant "github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// mainContainerNames maps role names to the primary container name. Note the alert role's
// container is "alerter-server" (historical), while its server binary and config directory are
// "alert-server".
var mainContainerNames = map[string]string{
	dolphinv1alpha1.RoleMaster: "master-server",
	dolphinv1alpha1.RoleWorker: "worker-server",
	dolphinv1alpha1.RoleApi:    "api-server",
	dolphinv1alpha1.RoleAlert:  "alerter-server",
}

// legacyLogVolumeSize is the shared log emptyDir SizeLimit the pre-migration operator rendered
// (3 x the 10Mi max log file size).
const legacyLogVolumeSize = "30Mi"

const (
	// zkConnectStringEnvName is the ZooKeeper discovery env var every server reads.
	zkConnectStringEnvName = "REGISTRY_ZOOKEEPER_CONNECT_STRING"

	// configVolumeName is the framework-owned role group ConfigMap volume.
	configVolumeName = "config"

	// curlCommand is the probe binary of the legacy actuator exec probes.
	curlCommand = "curl"
)

// DolphinSchedulerRoleGroupHandler builds the resources of a DolphinScheduler role group on top
// of reconciler.BaseRoleGroupHandler: the base handler owns labels, Services, the workload
// (StatefulSet for master/worker, Deployment for api/alert) and the ConfigMap skeleton, while
// this handler contributes the product specifics through the declared handler fields and the
// MainContainerCustomizer.
type DolphinSchedulerRoleGroupHandler struct {
	reconciler.BaseRoleGroupHandler[*dolphinv1alpha1.DolphinschedulerCluster]
}

var _ reconciler.RoleGroupHandler[*dolphinv1alpha1.DolphinschedulerCluster] = &DolphinSchedulerRoleGroupHandler{}

// productImageDefaults supplies whatever spec.image leaves empty, evaluated every reconcile so
// an operator upgrade moves clusters onto the co-released product image.
func productImageDefaults() commonsv1alpha1.ImageSpec {
	return commonsv1alpha1.ImageSpec{
		Repo:            dolphinv1alpha1.DefaultRepository,
		ProductVersion:  dolphinv1alpha1.DefaultProductVersion,
		KubedoopVersion: version.BuildVersion,
	}
}

// resolveProductImage resolves the container image and pull policy for the cluster from
// spec.image folded over the product defaults (also used by the DB-init Job).
func resolveProductImage(cr *dolphinv1alpha1.DolphinschedulerCluster) (string, corev1.PullPolicy, error) {
	spec := cr.GetSpec().Image
	image, err := spec.ResolveImage(dolphinv1alpha1.DefaultProductName, productImageDefaults())
	if err != nil {
		return "", "", err
	}
	return image, spec.ResolvedPullPolicy(productImageDefaults()), nil
}

// NewDolphinSchedulerRoleGroupHandler creates the handler with every reconcile-invariant
// setting: ports, workload kinds, container names, logging declarations and identity labels.
func NewDolphinSchedulerRoleGroupHandler(scheme *runtime.Scheme) *DolphinSchedulerRoleGroupHandler {
	h := &DolphinSchedulerRoleGroupHandler{}
	h.Scheme = scheme
	h.ImagePullPolicy = corev1.PullIfNotPresent
	h.ProductName = dolphinv1alpha1.DefaultProductName
	h.ImageDefaults = productImageDefaults()
	h.LabelDomain = LabelDomain
	// Keep the legacy shared log emptyDir size (the framework default is larger).
	h.LogVolumeSize = legacyLogVolumeSize

	// NOTE: StorageMountPath is deliberately NOT set. It is handler-global, and the legacy
	// operator rendered no data PVC for master (and never mounted worker's) — setting it would
	// give every StatefulSet role a PVC and break parity. Framework gap: per-role storage.

	for role, container := range mainContainerNames {
		h.SetRoleMainContainerName(role, container)
		h.SetRoleLoggingContainers(role, []productlogging.ContainerLogging{{
			Container:   container,
			Framework:   productlogging.LoggingFrameworkLogback,
			FileName:    dolphinv1alpha1.LogbackPropertiesFileName,
			Pattern:     dolphinv1alpha1.ConsoleConversionPattern,
			LogFileName: fmt.Sprintf("%s.log4j.xml", container),
		}})
	}

	// api and alert are stateless fronts over the database/registry: they have always been
	// Deployments and the e2e suites pin the kind.
	h.SetRoleWorkloadKind(dolphinv1alpha1.RoleApi, reconciler.WorkloadKindDeployment)
	h.SetRoleWorkloadKind(dolphinv1alpha1.RoleAlert, reconciler.WorkloadKindDeployment)

	// Container and Service ports, byte-identical (names, numbers, order) to the legacy
	// rendering (the order is the legacy sorted-by-name order).
	rolePorts := map[string][]corev1.ContainerPort{
		dolphinv1alpha1.RoleMaster: {
			{Name: dolphinv1alpha1.MasterActualPortName, ContainerPort: dolphinv1alpha1.MasterActualPort, Protocol: corev1.ProtocolTCP},
			{Name: dolphinv1alpha1.MasterPortName, ContainerPort: dolphinv1alpha1.MasterPort, Protocol: corev1.ProtocolTCP},
		},
		dolphinv1alpha1.RoleWorker: {
			{Name: dolphinv1alpha1.WorkerActualPortName, ContainerPort: dolphinv1alpha1.WorkerActualPort, Protocol: corev1.ProtocolTCP},
			{Name: dolphinv1alpha1.WorkerPortName, ContainerPort: dolphinv1alpha1.WorkerPort, Protocol: corev1.ProtocolTCP},
		},
		dolphinv1alpha1.RoleApi: {
			{Name: dolphinv1alpha1.ApiPortName, ContainerPort: dolphinv1alpha1.ApiPort, Protocol: corev1.ProtocolTCP},
			{Name: dolphinv1alpha1.ApiPythonPortName, ContainerPort: dolphinv1alpha1.ApiPythonPort, Protocol: corev1.ProtocolTCP},
		},
		dolphinv1alpha1.RoleAlert: {
			{Name: dolphinv1alpha1.AlerterActualPortName, ContainerPort: dolphinv1alpha1.AlerterActualPort, Protocol: corev1.ProtocolTCP},
			{Name: dolphinv1alpha1.AlerterPortName, ContainerPort: dolphinv1alpha1.AlerterPort, Protocol: corev1.ProtocolTCP},
		},
	}
	for role, ports := range rolePorts {
		h.SetRoleContainerPorts(role, ports)
		svcPorts := make([]corev1.ServicePort, 0, len(ports))
		for _, p := range ports {
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       p.Name,
				Port:       p.ContainerPort,
				Protocol:   p.Protocol,
				TargetPort: intstr.FromString(p.Name),
			})
		}
		h.SetRoleServicePorts(role, svcPorts)
	}

	return h
}

// BuildResources builds all resources of one DolphinScheduler role group.
func (h *DolphinSchedulerRoleGroupHandler) BuildResources(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cr *dolphinv1alpha1.DolphinschedulerCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (*reconciler.RoleGroupResources, error) {
	if _, known := mainContainerNames[buildCtx.RoleName]; !known {
		return nil, fmt.Errorf("unsupported role: %s", buildCtx.RoleName)
	}

	clusterConfig := cr.Spec.ClusterConfig
	if clusterConfig == nil {
		return nil, fmt.Errorf("spec.clusterConfig is required")
	}

	// Fill the product defaults (resources, anti-affinity, graceful shutdown) the framework
	// does not supply, before the base handler consumes the config.
	if err := h.ensureRoleGroupConfigDefaults(cr, buildCtx); err != nil {
		return nil, err
	}

	// Authentication (api role only): resolves the AuthenticationClass into container env and,
	// for LDAP, a CSI bind-credentials volume plus a credentials-export prologue for the start
	// script.
	var auth *security.AuthenticationResult
	if buildCtx.RoleName == dolphinv1alpha1.RoleApi && clusterConfig.Authentication != nil {
		var err error
		auth, err = security.Authentication(ctx, k8sClient, clusterConfig.Authentication)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve authentication: %w", err)
		}
		if auth.Provisioner != nil {
			buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, auth.Provisioner)
		}
	}

	// Every product-specific edit of the primary container goes through the customizer, which
	// runs before podOverrides so user overrides keep the last word.
	buildCtx.MainContainerCustomizer = h.mainContainerCustomizer(cr, buildCtx, auth)

	res, err := h.BaseRoleGroupHandler.BuildResources(ctx, k8sClient, cr, buildCtx)
	if err != nil {
		return nil, err
	}

	// common.properties: re-render the merged key set (product defaults < role < group
	// overrides, from MergedConfig) with the legacy sorted "k=v" serializer, applying the
	// S3-derived keys last — byte-identical content to the legacy ConfigMap.
	commonProperties, err := h.renderCommonProperties(ctx, k8sClient, cr, buildCtx)
	if err != nil {
		return nil, err
	}
	res.ConfigMap.Data[dolphinv1alpha1.DolphinCommonPropertiesName] = commonProperties

	// Metrics Service: byte-exact legacy shape (name, headless ClusterIP, prometheus labels and
	// annotations, the named-and-dangling targetPort "metrics", and the legacy label selector) —
	// pinned by the observability e2e suite.
	metricsService, err := h.buildMetricsService(buildCtx)
	if err != nil {
		return nil, err
	}
	res.MetricsService = metricsService

	return res, nil
}

// ensureRoleGroupConfigDefaults writes the product defaults into the (already role-folded) role
// group config when unset: legacy per-role resources, the weight-70 hostname anti-affinity and
// the 120s graceful shutdown.
func (h *DolphinSchedulerRoleGroupHandler) ensureRoleGroupConfigDefaults(
	cr *dolphinv1alpha1.DolphinschedulerCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) error {
	cfg := buildCtx.RoleGroupSpec.Config
	if cfg == nil {
		cfg = &commonsv1alpha1.RoleGroupConfigSpec{}
		buildCtx.RoleGroupSpec.Config = cfg
	}

	defaults, err := roleResourceDefaults(buildCtx.RoleName)
	if err != nil {
		return err
	}
	if cfg.Resources == nil {
		cfg.Resources = defaults
	} else {
		if cfg.Resources.CPU == nil {
			cfg.Resources.CPU = defaults.CPU
		}
		if cfg.Resources.Memory == nil {
			cfg.Resources.Memory = defaults.Memory
		}
		if cfg.Resources.Storage == nil {
			cfg.Resources.Storage = defaults.Storage
		}
	}

	if cfg.Affinity == nil {
		affinity, err := defaultRoleAffinity(cr.Name, buildCtx.RoleName)
		if err != nil {
			return err
		}
		cfg.Affinity = affinity
	}

	if cfg.GracefulShutdownTimeout == nil {
		cfg.GracefulShutdownTimeout = ptr.To(defaultGracefulShutdown)
	}

	return nil
}

// roleConfigPath returns the config file path inside the container:
// /kubedoop/dolphinscheduler/<role>-server/conf/<file>.
func roleConfigPath(roleName, fileName string) string {
	return path.Join(opgoconstant.KubedoopRoot, dolphinv1alpha1.DefaultProductName, roleServerName(roleName), "conf", fileName)
}

// envsConfigMapName is the cluster-wide env ConfigMap every container envFroms.
func envsConfigMapName(clusterName string) string {
	return clusterName + "-envs"
}

// mainContainerCustomizer reproduces the legacy primary container exactly: command and start
// script, sorted role env plus the ZooKeeper discovery env, the envFrom on "<cluster>-envs",
// the actuator exec probes and the subPath config mounts.
func (h *DolphinSchedulerRoleGroupHandler) mainContainerCustomizer(
	cr *dolphinv1alpha1.DolphinschedulerCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
	auth *security.AuthenticationResult,
) func(c *corev1.Container) error {
	roleName := buildCtx.RoleName
	zookeeperConfigMapName := cr.Spec.ClusterConfig.ZookeeperConfigMapName

	return func(c *corev1.Container) error {
		metricsPort, err := metricsPortForRole(roleName)
		if err != nil {
			return err
		}

		// Command and start script. The LDAP credentials-export prologue precedes the single
		// start block (the legacy api rendering duplicated the block; deliberately fixed, D12).
		script := mainContainerScript(roleName)
		if auth != nil && auth.LdapExportCommand != "" {
			script = auth.LdapExportCommand + "\n" + script
		}
		c.Command = slices.Clone(containerCommand)
		// Keep whatever args the builder already placed (user cliOverrides) after the script.
		c.Args = append([]string{script}, c.Args...)

		// Product env first (sorted role defaults, then auth env, then the ZooKeeper discovery
		// env), then the builder-supplied env (user envOverrides and product env defaults from
		// ProductConfig), which keeps the user's values winning at runtime.
		env := roleEnvDefaults(roleName)
		if auth != nil {
			env = append(env, auth.EnvVars...)
		}
		env = append(env, corev1.EnvVar{
			Name: zkConnectStringEnvName,
			ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: zookeeperConfigMapName},
					Key:                  constant.ZookeeperDiscoveryKey,
				},
			},
		})
		c.Env = append(env, c.Env...)

		c.EnvFrom = append(c.EnvFrom, corev1.EnvFromSource{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: envsConfigMapName(buildCtx.ClusterName)},
			},
		})

		c.ReadinessProbe = actuatorProbe(metricsPort, "readiness")
		c.LivenessProbe = actuatorProbe(metricsPort, "liveness")
		c.StartupProbe = startupProbe(metricsPort)

		// Mounts: DolphinScheduler reads its config files at fixed paths inside the server's
		// conf directory, so replace the framework's whole-directory config mount with two
		// subPath mounts of the same "config" volume (the role group ConfigMap). Every other
		// mount (CSI credential volumes from VolumeProviders) is kept.
		mounts := make([]corev1.VolumeMount, 0, len(c.VolumeMounts)+2)
		mounts = append(mounts,
			corev1.VolumeMount{
				Name:      configVolumeName,
				MountPath: roleConfigPath(roleName, dolphinv1alpha1.DolphinCommonPropertiesName),
				SubPath:   dolphinv1alpha1.DolphinCommonPropertiesName,
			},
			corev1.VolumeMount{
				Name:      configVolumeName,
				MountPath: roleConfigPath(roleName, dolphinv1alpha1.LogbackPropertiesFileName),
				SubPath:   dolphinv1alpha1.LogbackPropertiesFileName,
			},
		)
		for _, m := range c.VolumeMounts {
			if m.Name == configVolumeName && m.SubPath == "" {
				continue
			}
			mounts = append(mounts, m)
		}
		c.VolumeMounts = mounts

		return nil
	}
}

// actuatorProbe is the legacy Spring actuator exec probe (curl, 30s delay, 30s period).
func actuatorProbe(port int32, endpoint string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					curlCommand,
					fmt.Sprintf("http://localhost:%d/actuator/health/%s", port, endpoint),
				},
			},
		},
		InitialDelaySeconds: 30,
		SuccessThreshold:    1,
		FailureThreshold:    3,
		PeriodSeconds:       30,
		TimeoutSeconds:      5,
	}
}

// startupProbe is new relative to the legacy rendering (zookeeper-operator precedent): it gives
// a slow first start up to 10 minutes before the readiness/liveness budgets apply, so a cold
// database or registry cannot crash-loop the servers.
func startupProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					curlCommand,
					fmt.Sprintf("http://localhost:%d/actuator/health/readiness", port),
				},
			},
		},
		PeriodSeconds:    10,
		FailureThreshold: 60,
		SuccessThreshold: 1,
		TimeoutSeconds:   5,
	}
}

// renderCommonProperties serializes the merged common.properties (with the S3 overlay applied
// last) using the legacy sorted "key=value" serializer.
func (h *DolphinSchedulerRoleGroupHandler) renderCommonProperties(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cr *dolphinv1alpha1.DolphinschedulerCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (string, error) {
	props := map[string]string{}
	if buildCtx.MergedConfig != nil {
		maps.Copy(props, buildCtx.MergedConfig.ConfigFiles[dolphinv1alpha1.DolphinCommonPropertiesName])
	}

	if s3Spec := cr.Spec.ClusterConfig.S3; s3Spec != nil {
		s3Config, err := util.NewS3ConfigExtractor(k8sClient, s3Spec, cr.Namespace).GetS3Config(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get s3 config: %w", err)
		}
		props["resource.storage.type"] = "S3"
		props["resource.aws.access.key.id"] = s3Config.AccessKeyID
		// Region is never populated by the extractor; the empty value is kept for parity with
		// the legacy rendering (known defect, tracked separately).
		props["resource.aws.region"] = s3Config.Region
		props["resource.aws.s3.bucket.name"] = s3Config.BucketName
		props["resource.aws.s3.endpoint"] = s3Config.Endpoint
		props["resource.aws.secret.access.key"] = s3Config.SecretAccessKey
	}

	return util.ToProperties(props), nil
}

// buildMetricsService assembles the metrics Service slot in the exact legacy shape.
func (h *DolphinSchedulerRoleGroupHandler) buildMetricsService(buildCtx *reconciler.RoleGroupBuildContext) (*corev1.Service, error) {
	metricsPort, err := metricsPortForRole(buildCtx.RoleName)
	if err != nil {
		return nil, err
	}

	labels := legacyRoleGroupLabels(buildCtx.ClusterName, buildCtx.RoleName, buildCtx.RoleGroupName)
	labels["prometheus.io/scrape"] = defaultEnabled

	annotations := map[string]string{
		"prometheus.io/scrape": defaultEnabled,
		"prometheus.io/path":   "/actuator/prometheus",
		"prometheus.io/port":   strconv.Itoa(int(metricsPort)),
		"prometheus.io/scheme": "http",
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        buildCtx.ResourceName + "-metrics",
			Namespace:   buildCtx.ClusterNamespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{
					Name:     dolphinv1alpha1.MetricsPortName,
					Port:     metricsPort,
					Protocol: corev1.ProtocolTCP,
					// The named targetPort matches no container port (none is named "metrics");
					// the dangling reference is deliberate — the e2e suite pins the full shape.
					TargetPort: intstr.FromString(dolphinv1alpha1.MetricsPortName),
				},
			},
			Selector: legacyRoleGroupLabels(buildCtx.ClusterName, buildCtx.RoleName, buildCtx.RoleGroupName),
		},
	}, nil
}
