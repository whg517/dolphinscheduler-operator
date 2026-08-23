package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	dolphinv1alpha1 "github.com/zncdatadev/dolphinscheduler-operator/api/v1alpha1"
	"github.com/zncdatadev/dolphinscheduler-operator/pkg/util"
	opcommon "github.com/zncdatadev/operator-go/pkg/common"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// +kubebuilder:rbac:groups=dolphinscheduler.kubedoop.dev,resources=dolphinschedulerclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dolphinscheduler.kubedoop.dev,resources=dolphinschedulerclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dolphinscheduler.kubedoop.dev,resources=dolphinschedulerclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=secrets.kubedoop.dev,resources=secretclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=s3.kubedoop.dev,resources=s3connections,verbs=get;list;watch
// +kubebuilder:rbac:groups=authentication.kubedoop.dev,resources=authenticationclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

const (
	// dbInitJobContainerName / dbInitWaitContainerName are the DB-init Job container names.
	dbInitJobContainerName  = "dolphinscheduler-db-init-job"
	dbInitWaitContainerName = "wait-for-database"

	// dbInitWaitImage is the init container image probing the database port.
	dbInitWaitImage = "busybox:1.30.1"

	// dbInitWaitReason / dbInitWaitMessage are the Waiting condition of the DB-init gate. The
	// message is deliberately CONSTANT across passes: the status write is DeepEqual-guarded and
	// this path returns no error, so a varying message would rewrite the CR forever. The
	// namespace/name go into a log line instead.
	dbInitWaitReason  = "WaitingForDBInit"
	dbInitWaitMessage = "the database schema initialization Job has not completed"

	// dbInitRequeueAfter is how long to wait between DB-init Job checks.
	dbInitRequeueAfter = 15 * time.Second
)

// ClusterExtension provisions the cluster-scoped resources the role groups depend on: the
// "<cluster>-envs" ConfigMap and the create-once DB schema init Job. PreReconcile returns a
// typed wait until the Job has succeeded, so the roles wait for the schema — the legacy
// ordering. It also guards the Deployment-to-StatefulSet upgrade: a leftover controller-owned
// api/alert Deployment fails the cluster with an explicit runbook error.
type ClusterExtension struct {
	scheme *runtime.Scheme
}

var _ opcommon.ClusterExtension[*dolphinv1alpha1.DolphinschedulerCluster] = &ClusterExtension{}

// NewClusterExtension creates the cluster extension.
func NewClusterExtension(scheme *runtime.Scheme) *ClusterExtension {
	return &ClusterExtension{scheme: scheme}
}

// Name implements opcommon.Extension.
func (e *ClusterExtension) Name() string { return "dolphinscheduler-cluster" }

// PreReconcile implements opcommon.ClusterExtension.
func (e *ClusterExtension) PreReconcile(ctx context.Context, c ctrlclient.Client, cr *dolphinv1alpha1.DolphinschedulerCluster) error {
	if err := e.ensureNoLegacyDeployments(ctx, c, cr); err != nil {
		return err
	}
	if err := e.ensureEnvsConfigMap(ctx, c, cr); err != nil {
		return err
	}
	return e.ensureDBInitJob(ctx, c, cr)
}

// PostReconcile implements opcommon.ClusterExtension.
func (e *ClusterExtension) PostReconcile(_ context.Context, _ ctrlclient.Client, _ *dolphinv1alpha1.DolphinschedulerCluster) error {
	return nil
}

// OnReconcileError implements opcommon.ClusterExtension.
func (e *ClusterExtension) OnReconcileError(_ context.Context, _ ctrlclient.Client, _ *dolphinv1alpha1.DolphinschedulerCluster, _ error) error {
	return nil
}

// ensureNoLegacyDeployments is the Deployment-to-StatefulSet upgrade guard. Pre-0.13 operators
// rendered the api/alert roles as Deployments; the same-named StatefulSets this operator builds
// do NOT conflict with them (different resources), so both would run at once behind the same
// selector — every client Service serving doubled endpoints, half of them pods of a template
// never updated again. Detect-and-fail: the reconcile is blocked with an explicit runbook error
// until the leftover Deployments are deleted manually.
func (e *ClusterExtension) ensureNoLegacyDeployments(ctx context.Context, c ctrlclient.Client, cr *dolphinv1alpha1.DolphinschedulerCluster) error {
	spec := cr.GetSpec()

	var leftovers []string
	for _, roleName := range []string{dolphinv1alpha1.RoleApi, dolphinv1alpha1.RoleAlert} {
		roleSpec, declared := spec.Roles[roleName]
		if !declared {
			continue
		}
		for groupName := range roleSpec.RoleGroups {
			name := reconciler.RoleGroupResourceName(cr.Name, roleName, groupName)
			deployment := &appsv1.Deployment{}
			err := c.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: name}, deployment)
			switch {
			case apierrors.IsNotFound(err):
				continue
			case err != nil:
				return fmt.Errorf("failed to check for a legacy Deployment %s/%s: %w", cr.Namespace, name, err)
			}
			// Only a Deployment this CR controller-owns is the legacy workload; anything else
			// merely shares a name and is none of our business.
			if metav1.IsControlledBy(deployment, cr) {
				leftovers = append(leftovers, name)
			}
		}
	}
	if len(leftovers) == 0 {
		return nil
	}

	slices.Sort(leftovers)
	return fmt.Errorf(
		"legacy Deployment workload(s) %s from a pre-StatefulSet operator version still exist: "+
			"the api/alert roles now render StatefulSets with the same names and selectors, and both "+
			"running at once doubles every Service's endpoints; run "+
			"`kubectl -n %s delete deployment %s` and the reconcile will proceed",
		strings.Join(leftovers, ", "), cr.Namespace, strings.Join(leftovers, " "))
}

// ensureEnvsConfigMap applies the "<cluster>-envs" ConfigMap every container (and the DB-init
// Job) envFroms: the master role's envOverrides merged with the product defaults (defaults win
// — the legacy precedence, kept byte for byte) plus the database-derived Spring datasource env.
func (e *ClusterExtension) ensureEnvsConfigMap(ctx context.Context, c ctrlclient.Client, cr *dolphinv1alpha1.DolphinschedulerCluster) error {
	data := map[string]string{}

	// The legacy operator built this ConfigMap from the MASTER role's env overrides only
	// (role-level, then role-group-level). Groups are folded in sorted name order so the
	// result is deterministic where the legacy map iteration was not.
	lastGroup := defaultRoleGroupName
	if master := cr.Spec.Master; master != nil {
		if master.OverridesSpec != nil {
			maps.Copy(data, master.EnvOverrides)
		}
		for _, groupName := range slices.Sorted(maps.Keys(master.RoleGroups)) {
			if o := master.RoleGroups[groupName].OverridesSpec; o != nil {
				maps.Copy(data, o.EnvOverrides)
			}
			lastGroup = groupName
		}
	}

	// Product defaults deliberately WIN over user env overrides here (legacy behavior).
	maps.Copy(data, defaultEnvOverrides())

	if clusterConfig := cr.Spec.ClusterConfig; clusterConfig != nil && clusterConfig.Database != nil {
		dbSpec := clusterConfig.Database
		extractor := util.NewDataBaseExtractor(c, &dbSpec.ConnectionString)
		if dbSpec.CredentialsSecret != "" {
			extractor = extractor.CredentialsInSecret(dbSpec.CredentialsSecret, cr.Namespace)
		}
		dbConfig, err := extractor.ExtractDatabaseInfo(ctx)
		if err != nil {
			return fmt.Errorf("failed to extract database info: %w", err)
		}
		maps.Copy(data, map[string]string{
			"DATABASE":                            dbConfig.DbType,
			"SPRING_DATASOURCE_URL":               fmt.Sprintf("jdbc:%s://%s:%s/%s", dbConfig.DbType, dbConfig.Host, dbConfig.Port, dbConfig.DbName),
			"SPRING_DATASOURCE_USERNAME":          dbConfig.Username,
			"SPRING_DATASOURCE_PASSWORD":          dbConfig.Password,
			"SPRING_DATASOURCE_DRIVER-CLASS-NAME": dbConfig.Driver,
		})
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      envsConfigMapName(cr.Name),
		Namespace: cr.Namespace,
	}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, cm, func() error {
		cm.Labels = legacyRoleGroupLabels(cr.Name, dolphinv1alpha1.RoleMaster, lastGroup)
		cm.Data = data
		return controllerutil.SetControllerReference(cr, cm, e.scheme)
	}); err != nil {
		return fmt.Errorf("failed to ensure env configmap %s/%s: %w", cr.Namespace, envsConfigMapName(cr.Name), err)
	}

	return nil
}

// ensureDBInitJob creates the DB schema init Job (name = the cluster name) once and returns a
// typed wait until it has succeeded, so the role groups are not reconciled before the schema
// exists (Waiting condition, no Degraded, no Warning event). A Job's spec is immutable: the Job
// is never updated, and a concurrent create is tolerated.
func (e *ClusterExtension) ensureDBInitJob(ctx context.Context, c ctrlclient.Client, cr *dolphinv1alpha1.DolphinschedulerCluster) error {
	clusterConfig := cr.Spec.ClusterConfig
	if clusterConfig == nil || clusterConfig.Database == nil {
		log.FromContext(ctx).Info("no database configured; skipping DB init job")
		return nil
	}

	existing := &batchv1.Job{}
	err := c.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, existing)
	switch {
	case apierrors.IsNotFound(err):
		job, buildErr := e.buildDBInitJob(cr)
		if buildErr != nil {
			return buildErr
		}
		if err := controllerutil.SetControllerReference(cr, job, e.scheme); err != nil {
			return err
		}
		if err := c.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create DB init job %s/%s: %w", cr.Namespace, cr.Name, err)
		}
		log.FromContext(ctx).Info("waiting for DB init job to complete", "namespace", cr.Namespace, "job", cr.Name)
		return opcommon.NewRequeueAfterError(dbInitRequeueAfter, dbInitWaitReason, dbInitWaitMessage)
	case err != nil:
		return fmt.Errorf("failed to get DB init job %s/%s: %w", cr.Namespace, cr.Name, err)
	}

	if existing.Status.Succeeded > 0 {
		return nil
	}
	log.FromContext(ctx).Info("waiting for DB init job to complete", "namespace", cr.Namespace, "job", cr.Name)
	return opcommon.NewRequeueAfterError(dbInitRequeueAfter, dbInitWaitReason, dbInitWaitMessage)
}

// buildDBInitJob renders the legacy DB-init Job: a busybox init container waiting for the
// database port (5432 hardcoded, legacy parity) and the product image running
// tools/bin/upgrade-schema.sh with the "<cluster>-envs" environment.
func (e *ClusterExtension) buildDBInitJob(cr *dolphinv1alpha1.DolphinschedulerCluster) (*batchv1.Job, error) {
	image, pullPolicy, err := resolveProductImage(cr)
	if err != nil {
		return nil, err
	}

	dbHost, _ := util.GetDatabaseHost(cr.Spec.ClusterConfig.Database.ConnectionString)
	labels := legacyClusterLabels(cr.Name)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:    ptr.To[int64](1001),
						RunAsGroup:   ptr.To[int64](1001),
						RunAsNonRoot: ptr.To(false),
					},
					InitContainers: []corev1.Container{
						{
							Name:  dbInitWaitContainerName,
							Image: dbInitWaitImage,
							Command: []string{
								"sh",
								"-xc",
								fmt.Sprintf("for i in $(seq 1 180); do nc -z -w3 %s 5432 && exit 0 || sleep 5; done; exit 1", dbHost),
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            dbInitJobContainerName,
							Image:           image,
							ImagePullPolicy: pullPolicy,
							Command:         slices.Clone(containerCommand),
							Args:            []string{"chmod +x tools/bin/upgrade-schema.sh && tools/bin/upgrade-schema.sh"},
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: envsConfigMapName(cr.Name)},
									},
								},
							},
						},
					},
				},
			},
		},
	}, nil
}
