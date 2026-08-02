package controller

// LabelDomain is the product domain for the framework's identity (selector) labels:
// dolphinscheduler.kubedoop.dev/{cluster,role,role-group}.
const LabelDomain = "dolphinscheduler.kubedoop.dev"

// legacyManagedBy and legacyName are the app.kubernetes.io/managed-by and app.kubernetes.io/name
// values the pre-framework operator stamped on every resource (managed-by = the CRD group,
// name = the lowercase CRD kind). The metrics Service keeps them byte for byte (D6): its shape,
// including the legacy selector, is pinned by the observability e2e suite and by existing scrape
// configurations. The RBAC objects, env ConfigMap and DB-init Job keep them too.
const (
	legacyManagedBy = "dolphinscheduler.kubedoop.dev"
	legacyName      = "dolphinschedulercluster"
)

// The app.kubernetes.io label keys used by the legacy label sets and the default affinity.
const (
	labelInstanceKey  = "app.kubernetes.io/instance"
	labelNameKey      = "app.kubernetes.io/name"
	labelManagedByKey = "app.kubernetes.io/managed-by"
	labelComponentKey = "app.kubernetes.io/component"
	labelRoleGroupKey = "app.kubernetes.io/role-group"
)

// legacyRoleGroupLabels reproduces operator-go v0.12's RoleGroupInfo.GetLabels(): the five
// app.kubernetes.io labels every role-group resource used to carry.
func legacyRoleGroupLabels(clusterName, roleName, groupName string) map[string]string {
	return map[string]string{
		labelInstanceKey:  clusterName,
		labelNameKey:      legacyName,
		labelManagedByKey: legacyManagedBy,
		labelComponentKey: roleName,
		labelRoleGroupKey: groupName,
	}
}

// legacyClusterLabels reproduces operator-go v0.12's ClusterInfo.GetLabels(): the three
// cluster-scoped labels the RBAC objects, the env ConfigMap and the DB-init Job carried.
func legacyClusterLabels(clusterName string) map[string]string {
	return map[string]string{
		labelInstanceKey:  clusterName,
		labelNameKey:      legacyName,
		labelManagedByKey: legacyManagedBy,
	}
}
