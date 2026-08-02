package controller

import (
	"context"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dolphinv1alpha1 "github.com/zncdatadev/dolphinscheduler-operator/api/v1alpha1"
	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testClusterName = "test-dolphinscheduler"
	testNamespace   = "default"
)

func newTestCluster() *dolphinv1alpha1.DolphinschedulerCluster {
	pullPolicy := corev1.PullIfNotPresent
	roleSpec := func() *dolphinv1alpha1.RoleSpec {
		return &dolphinv1alpha1.RoleSpec{
			RoleGroups: map[string]dolphinv1alpha1.RoleGroupSpec{
				defaultRoleGroupName: {Replicas: ptr.To[int32](1)},
			},
		}
	}
	return &dolphinv1alpha1.DolphinschedulerCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterName,
			Namespace: testNamespace,
		},
		Spec: dolphinv1alpha1.DolphinschedulerClusterSpec{
			Image: &dolphinv1alpha1.ImageSpec{
				Repo:       dolphinv1alpha1.DefaultRepository,
				PullPolicy: &pullPolicy,
			},
			ClusterConfig: &dolphinv1alpha1.ClusterConfigSpec{
				ZookeeperConfigMapName: "test-znode",
				Database: &dolphinv1alpha1.DatabaseSpec{
					ConnectionString:  "jdbc:postgresql://postgresql:5432/dolphinscheduler",
					DatabaseType:      "postgresql",
					CredentialsSecret: "postgresql-credentials",
				},
			},
			Master:  roleSpec(),
			Worker:  roleSpec(),
			Api:     roleSpec(),
			Alerter: roleSpec(),
		},
	}
}

// newBuildContext assembles a RoleGroupBuildContext the way GenericReconciler does (product
// config lowest, then role, then group overrides; role config folded into the group).
func newBuildContext(cr *dolphinv1alpha1.DolphinschedulerCluster, roleName string) *reconciler.RoleGroupBuildContext {
	groupName := defaultRoleGroupName
	spec := cr.GetSpec()
	roleSpec := spec.Roles[roleName]
	groupSpec := roleSpec.RoleGroups[groupName]

	merged := config.NewConfigMerger().Merge(
		ProductConfig(cr, roleName, groupName),
		roleSpec.GetOverrides(),
		groupSpec.GetOverrides(),
	)
	merged.Logging = productlogging.MergeLoggingSpec(roleSpec.GetConfig().Logging, groupSpec.GetConfig().Logging)

	mergedGroupSpec := groupSpec.DeepCopy()
	mergedGroupSpec.Config = reconciler.MergeRoleGroupConfig(roleSpec.GetConfig(), groupSpec.GetConfig())

	return &reconciler.RoleGroupBuildContext{
		ClusterName:        cr.GetName(),
		ClusterNamespace:   cr.GetNamespace(),
		ClusterLabels:      map[string]string{},
		ClusterSpec:        spec,
		RoleName:           roleName,
		RoleSpec:           &roleSpec,
		RoleGroupName:      groupName,
		RoleGroupSpec:      *mergedGroupSpec,
		MergedConfig:       merged,
		ResourceName:       reconciler.RoleGroupResourceName(cr.GetName(), roleName, groupName),
		ServiceAccountName: dolphinv1alpha1.DefaultProductName,
	}
}

func newFakeClient(objs ...ctrlclient.Object) ctrlclient.Client {
	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(dolphinv1alpha1.AddToScheme(scheme)).To(Succeed())
	Expect(authv1alpha1.AddToScheme(scheme)).To(Succeed())
	return fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

var _ = Describe("DolphinSchedulerRoleGroupHandler", func() {
	var (
		handler *DolphinSchedulerRoleGroupHandler
		cr      *dolphinv1alpha1.DolphinschedulerCluster
		ctx     context.Context
	)

	BeforeEach(func() {
		handler = NewDolphinSchedulerRoleGroupHandler(clientgoscheme.Scheme)
		cr = newTestCluster()
		ctx = context.Background()
	})

	Describe("workload kinds", func() {
		It("declares StatefulSet for master/worker and Deployment for api/alert", func() {
			Expect(handler.WorkloadKindFor(dolphinv1alpha1.RoleMaster)).To(Equal(reconciler.WorkloadKindStatefulSet))
			Expect(handler.WorkloadKindFor(dolphinv1alpha1.RoleWorker)).To(Equal(reconciler.WorkloadKindStatefulSet))
			Expect(handler.WorkloadKindFor(dolphinv1alpha1.RoleApi)).To(Equal(reconciler.WorkloadKindDeployment))
			Expect(handler.WorkloadKindFor(dolphinv1alpha1.RoleAlert)).To(Equal(reconciler.WorkloadKindDeployment))
		})

		It("builds the e2e-pinned workload kinds and resource names", func() {
			byRole := map[string]struct {
				statefulSet bool
			}{
				dolphinv1alpha1.RoleMaster: {statefulSet: true},
				dolphinv1alpha1.RoleWorker: {statefulSet: true},
				dolphinv1alpha1.RoleApi:    {statefulSet: false},
				dolphinv1alpha1.RoleAlert:  {statefulSet: false},
			}
			for role, expect := range byRole {
				buildCtx := newBuildContext(cr, role)
				res, err := handler.BuildResources(ctx, newFakeClient(), cr, buildCtx)
				Expect(err).NotTo(HaveOccurred(), "role %s", role)

				name := fmt.Sprintf("%s-%s-default", testClusterName, role)
				if expect.statefulSet {
					Expect(res.StatefulSet).NotTo(BeNil(), "role %s", role)
					Expect(res.Deployment).To(BeNil(), "role %s", role)
					Expect(res.StatefulSet.Name).To(Equal(name))
				} else {
					Expect(res.Deployment).NotTo(BeNil(), "role %s", role)
					Expect(res.StatefulSet).To(BeNil(), "role %s", role)
					Expect(res.Deployment.Name).To(Equal(name))
				}
				Expect(res.ConfigMap.Name).To(Equal(name))
				Expect(res.Service.Name).To(Equal(name))
			}
		})
	})

	Describe("metrics service", func() {
		It("renders the exact legacy shape", func() {
			buildCtx := newBuildContext(cr, dolphinv1alpha1.RoleMaster)
			res, err := handler.BuildResources(ctx, newFakeClient(), cr, buildCtx)
			Expect(err).NotTo(HaveOccurred())

			svc := res.MetricsService
			Expect(svc).NotTo(BeNil())
			Expect(svc.Name).To(Equal(testClusterName + "-master-default-metrics"))
			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
			Expect(svc.Spec.Ports).To(ConsistOf(corev1.ServicePort{
				Name:       "metrics",
				Port:       5679,
				Protocol:   corev1.ProtocolTCP,
				TargetPort: intstr.FromString("metrics"),
			}))
			Expect(svc.Labels).To(HaveKeyWithValue("prometheus.io/scrape", defaultEnabled))
			Expect(svc.Annotations).To(Equal(map[string]string{
				"prometheus.io/scrape": defaultEnabled,
				"prometheus.io/path":   "/actuator/prometheus",
				"prometheus.io/port":   "5679",
				"prometheus.io/scheme": "http",
			}))
			// The selector keeps the full legacy label set (e2e asserts the instance+component
			// subset).
			Expect(svc.Spec.Selector).To(Equal(map[string]string{
				labelInstanceKey:  testClusterName,
				labelNameKey:      "dolphinschedulercluster",
				labelManagedByKey: "dolphinscheduler.kubedoop.dev",
				labelComponentKey: "master",
				labelRoleGroupKey: defaultRoleGroupName,
			}))
		})

		It("uses the role-specific metrics port", func() {
			ports := map[string]int32{
				dolphinv1alpha1.RoleMaster: 5679,
				dolphinv1alpha1.RoleWorker: 1235,
				dolphinv1alpha1.RoleApi:    12345,
				dolphinv1alpha1.RoleAlert:  50053,
			}
			for role, port := range ports {
				buildCtx := newBuildContext(cr, role)
				res, err := handler.BuildResources(ctx, newFakeClient(), cr, buildCtx)
				Expect(err).NotTo(HaveOccurred(), "role %s", role)
				Expect(res.MetricsService.Spec.Ports[0].Port).To(Equal(port), "role %s", role)
				Expect(res.MetricsService.Annotations["prometheus.io/port"]).To(Equal(fmt.Sprintf("%d", port)), "role %s", role)
			}
		})
	})

	Describe("master container", func() {
		var container corev1.Container

		BeforeEach(func() {
			buildCtx := newBuildContext(cr, dolphinv1alpha1.RoleMaster)
			res, err := handler.BuildResources(ctx, newFakeClient(), cr, buildCtx)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.StatefulSet.Spec.Template.Spec.Containers).To(HaveLen(1))
			container = res.StatefulSet.Spec.Template.Spec.Containers[0]
		})

		It("keeps the legacy container name, command and start script", func() {
			Expect(container.Name).To(Equal("master-server"))
			Expect(container.Command).To(Equal([]string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}))
			Expect(container.Args).To(HaveLen(1))
			script := container.Args[0]
			Expect(script).To(ContainSubstring("prepare_signal_handlers"))
			Expect(script).To(ContainSubstring("master-server/bin/start.sh &"))
			Expect(script).To(ContainSubstring("wait_for_termination $!"))
			Expect(script).To(ContainSubstring("rm -f /kubedoop/log/_vector/shutdown"))
			// The start block appears exactly once.
			Expect(strings.Count(script, "bin/start.sh &")).To(Equal(1))
		})

		It("renders the exact legacy env list plus the product default env", func() {
			names := make([]string, 0, len(container.Env))
			for _, e := range container.Env {
				names = append(names, e.Name)
			}
			Expect(names).To(Equal([]string{
				envJavaOpts,
				"MASTER_DISPATCH_TASK_NUM",
				"MASTER_EXEC_TASK_NUM",
				"MASTER_EXEC_THREADS",
				"MASTER_FAILOVER_INTERVAL",
				"MASTER_HEARTBEAT_ERROR_THRESHOLD",
				"MASTER_HOST_SELECTOR",
				"MASTER_KILL_APPLICATION_WHEN_HANDLE_FAILOVER",
				"MASTER_MAX_HEARTBEAT_INTERVAL",
				"MASTER_SERVER_LOAD_PROTECTION_ENABLED",
				"MASTER_SERVER_LOAD_PROTECTION_MAX_DISK_USAGE_PERCENTAGE_THRESHOLDS",
				"MASTER_SERVER_LOAD_PROTECTION_MAX_JVM_CPU_USAGE_PERCENTAGE_THRESHOLDS",
				"MASTER_SERVER_LOAD_PROTECTION_MAX_SYSTEM_CPU_USAGE_PERCENTAGE_THRESHOLDS",
				"MASTER_SERVER_LOAD_PROTECTION_MAX_SYSTEM_MEMORY_USAGE_PERCENTAGE_THRESHOLDS",
				"MASTER_STATE_WHEEL_INTERVAL",
				"MASTER_TASK_COMMIT_INTERVAL",
				"MASTER_TASK_COMMIT_RETRYTIMES",
				zkConnectStringEnvName,
				// From ProductConfig EnvOverrides (also present via envFrom "<cluster>-envs").
				"SPRING_JACKSON_TIME_ZONE",
				"TZ",
			}))

			Expect(container.Env[0].Value).To(Equal("-Xms512m -Xmx512m -Xmn256m"))

			var zkEnv *corev1.EnvVar
			for i := range container.Env {
				if container.Env[i].Name == zkConnectStringEnvName {
					zkEnv = &container.Env[i]
				}
			}
			Expect(zkEnv).NotTo(BeNil())
			Expect(zkEnv.ValueFrom.ConfigMapKeyRef.Name).To(Equal("test-znode"))
			Expect(zkEnv.ValueFrom.ConfigMapKeyRef.Key).To(Equal("ZOOKEEPER"))

			Expect(container.EnvFrom).To(HaveLen(1))
			Expect(container.EnvFrom[0].ConfigMapRef.Name).To(Equal(testClusterName + "-envs"))
		})

		It("keeps the legacy actuator probes and adds a startup probe", func() {
			Expect(container.ReadinessProbe).NotTo(BeNil())
			Expect(container.ReadinessProbe.Exec.Command).To(Equal([]string{
				curlCommand, "http://localhost:5679/actuator/health/readiness",
			}))
			Expect(container.ReadinessProbe.InitialDelaySeconds).To(Equal(int32(30)))
			Expect(container.ReadinessProbe.PeriodSeconds).To(Equal(int32(30)))
			Expect(container.ReadinessProbe.TimeoutSeconds).To(Equal(int32(5)))
			Expect(container.ReadinessProbe.FailureThreshold).To(Equal(int32(3)))
			Expect(container.ReadinessProbe.SuccessThreshold).To(Equal(int32(1)))

			Expect(container.LivenessProbe).NotTo(BeNil())
			Expect(container.LivenessProbe.Exec.Command).To(Equal([]string{
				curlCommand, "http://localhost:5679/actuator/health/liveness",
			}))

			Expect(container.StartupProbe).NotTo(BeNil())
			Expect(container.StartupProbe.Exec.Command).To(Equal([]string{
				curlCommand, "http://localhost:5679/actuator/health/readiness",
			}))
			Expect(container.StartupProbe.PeriodSeconds).To(Equal(int32(10)))
			Expect(container.StartupProbe.FailureThreshold).To(Equal(int32(60)))
		})

		It("mounts the config files at the legacy subPath targets", func() {
			Expect(container.VolumeMounts[0]).To(Equal(corev1.VolumeMount{
				Name:      configVolumeName,
				MountPath: "/kubedoop/dolphinscheduler/master-server/conf/common.properties",
				SubPath:   "common.properties",
			}))
			Expect(container.VolumeMounts[1]).To(Equal(corev1.VolumeMount{
				Name:      configVolumeName,
				MountPath: "/kubedoop/dolphinscheduler/master-server/conf/logback-spring.xml",
				SubPath:   "logback-spring.xml",
			}))
		})

		It("renders the legacy container ports and resources", func() {
			Expect(container.Ports).To(Equal([]corev1.ContainerPort{
				{Name: "actual-port", ContainerPort: 5679, Protocol: corev1.ProtocolTCP},
				{Name: "port", ContainerPort: 5678, Protocol: corev1.ProtocolTCP},
			}))
			Expect(container.Resources.Requests.Cpu().String()).To(Equal("300m"))
			Expect(container.Resources.Limits.Cpu().String()).To(Equal("500m"))
			Expect(container.Resources.Limits.Memory().String()).To(Equal("800Mi"))
		})
	})

	Describe("alert role", func() {
		It("keeps the historical alerter-server container name over the alert-server binary", func() {
			buildCtx := newBuildContext(cr, dolphinv1alpha1.RoleAlert)
			res, err := handler.BuildResources(ctx, newFakeClient(), cr, buildCtx)
			Expect(err).NotTo(HaveOccurred())
			container := res.Deployment.Spec.Template.Spec.Containers[0]
			Expect(container.Name).To(Equal("alerter-server"))
			Expect(container.Args[0]).To(ContainSubstring("alert-server/bin/start.sh &"))
			Expect(container.VolumeMounts[0].MountPath).To(
				Equal("/kubedoop/dolphinscheduler/alert-server/conf/common.properties"))
		})
	})

	Describe("config map", func() {
		It("renders common.properties with the legacy sorted serializer and defaults", func() {
			buildCtx := newBuildContext(cr, dolphinv1alpha1.RoleMaster)
			res, err := handler.BuildResources(ctx, newFakeClient(), cr, buildCtx)
			Expect(err).NotTo(HaveOccurred())

			Expect(res.ConfigMap.Data).To(HaveKey("common.properties"))
			Expect(res.ConfigMap.Data).To(HaveKey("logback-spring.xml"))

			properties := res.ConfigMap.Data["common.properties"]
			Expect(properties).To(ContainSubstring("data.basedir.path=/tmp/dolphinscheduler\n"))
			Expect(properties).To(ContainSubstring("resource.storage.type=LOCAL\n"))
			// Sorted, raw "k=v" lines (the legacy serializer, not the escaping properties adapter).
			Expect(properties).To(ContainSubstring("datasource.encryption.salt=!@#$%^&*\n"))
			lines := strings.Split(strings.TrimSpace(properties), "\n")
			Expect(lines).To(HaveLen(34))
		})

		It("lets user configOverrides win over the product defaults", func() {
			cr.Spec.Master.OverridesSpec = &commonsv1alpha1.OverridesSpec{
				ConfigOverrides: map[string]map[string]string{
					"common.properties": {"data.basedir.path": "/custom"},
				},
			}
			buildCtx := newBuildContext(cr, dolphinv1alpha1.RoleMaster)
			res, err := handler.BuildResources(ctx, newFakeClient(), cr, buildCtx)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.ConfigMap.Data["common.properties"]).To(ContainSubstring("data.basedir.path=/custom\n"))
		})
	})

	Describe("services", func() {
		It("keeps the legacy service port names and numbers", func() {
			buildCtx := newBuildContext(cr, dolphinv1alpha1.RoleWorker)
			res, err := handler.BuildResources(ctx, newFakeClient(), cr, buildCtx)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Service.Spec.Ports).To(Equal([]corev1.ServicePort{
				{Name: "actual-port", Port: 1235, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("actual-port")},
				{Name: "port", Port: 1234, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("port")},
			}))
		})
	})

	Describe("api role with LDAP authentication", func() {
		It("adds the LDAP env, the bind-credentials volume and a single start block", func() {
			authClass := &authv1alpha1.AuthenticationClass{
				ObjectMeta: metav1.ObjectMeta{Name: "ldap"},
				Spec: authv1alpha1.AuthenticationClassSpec{
					AuthenticationProvider: &authv1alpha1.AuthenticationProvider{
						LDAP: &authv1alpha1.LDAPProvider{
							Hostname: "openldap",
							Port:     1389,
							BindCredentials: &commonsv1alpha1.Credentials{
								SecretClass: "ldap-credentials",
							},
							SearchBase: "dc=example,dc=org",
						},
					},
				},
			}
			// AuthenticationClass is cluster-scoped in spirit but resolved in the CR namespace
			// by the legacy code path; the fake object carries no namespace.
			cr.Spec.ClusterConfig.Authentication = []dolphinv1alpha1.AuthenticationSpec{
				{AuthenticationClass: "ldap"},
			}

			buildCtx := newBuildContext(cr, dolphinv1alpha1.RoleApi)
			res, err := handler.BuildResources(ctx, newFakeClient(authClass), cr, buildCtx)
			Expect(err).NotTo(HaveOccurred())

			container := res.Deployment.Spec.Template.Spec.Containers[0]

			names := make([]string, 0, len(container.Env))
			zkEnvCount := 0
			for _, e := range container.Env {
				names = append(names, e.Name)
				if e.Name == "REGISTRY_ZOOKEEPER_CONNECT_STRING" {
					zkEnvCount++
				}
			}
			Expect(names).To(ContainElement("SECURITY_AUTHENTICATION_TYPE"))
			Expect(names).To(ContainElement("SECURITY_AUTHENTICATION_LDAP_URLS"))
			// The ZooKeeper env appears exactly once (the legacy api rendering duplicated it).
			Expect(zkEnvCount).To(Equal(1))

			// The credentials-export prologue precedes exactly one start block (the legacy api
			// rendering duplicated the block).
			script := container.Args[0]
			Expect(script).To(HavePrefix("export SECURITY_AUTHENTICATION_LDAP_USERNAME"))
			Expect(strings.Count(script, "bin/start.sh &")).To(Equal(1))

			// The bind-credentials CSI volume is mounted read-only at the legacy path.
			var ldapMount *corev1.VolumeMount
			for i := range container.VolumeMounts {
				if container.VolumeMounts[i].Name == dolphinv1alpha1.LdapBindCredintialsVolumeName {
					ldapMount = &container.VolumeMounts[i]
				}
			}
			Expect(ldapMount).NotTo(BeNil())
			Expect(ldapMount.MountPath).To(Equal("/kubedoop/secret/ldap-bind-credentials"))

			var ldapVolume *corev1.Volume
			podSpec := res.Deployment.Spec.Template.Spec
			for i := range podSpec.Volumes {
				if podSpec.Volumes[i].Name == dolphinv1alpha1.LdapBindCredintialsVolumeName {
					ldapVolume = &podSpec.Volumes[i]
				}
			}
			Expect(ldapVolume).NotTo(BeNil())
			pvcTemplate := ldapVolume.Ephemeral.VolumeClaimTemplate
			Expect(pvcTemplate.Annotations).To(HaveKeyWithValue("secrets.kubedoop.dev/class", "ldap-credentials"))
			Expect(pvcTemplate.Spec.Resources.Requests.Storage().String()).To(Equal("1Mi"))
		})
	})
})
