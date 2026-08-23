package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dolphinv1alpha1 "github.com/zncdatadev/dolphinscheduler-operator/api/v1alpha1"
	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// The Deployment-to-StatefulSet upgrade guard: a pre-0.13 operator rendered api/alert as
// Deployments; a same-named StatefulSet does not conflict with them, so the guard must fail the
// reconcile until the legacy workloads are deleted manually.
var _ = Describe("ClusterExtension upgrade guard", func() {
	var (
		ctx       context.Context
		scheme    *runtime.Scheme
		extension *ClusterExtension
		cr        *dolphinv1alpha1.DolphinschedulerCluster
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(dolphinv1alpha1.AddToScheme(scheme)).To(Succeed())
		Expect(authv1alpha1.AddToScheme(scheme)).To(Succeed())
		extension = NewClusterExtension(scheme)
		cr = newTestCluster()
	})

	It("fails with a runbook error while a controller-owned legacy api/alert Deployment exists", func() {
		deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterName + "-api-default",
			Namespace: testNamespace,
		}}
		Expect(controllerutil.SetControllerReference(cr, deployment, scheme)).To(Succeed())
		c := newFakeClient(deployment)

		err := extension.ensureNoLegacyDeployments(ctx, c, cr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(testClusterName + "-api-default"))
		Expect(err.Error()).To(ContainSubstring("kubectl -n " + testNamespace + " delete deployment"))
	})

	It("ignores a same-named Deployment the CR does not own", func() {
		deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterName + "-alert-default",
			Namespace: testNamespace,
		}}
		c := newFakeClient(deployment)

		Expect(extension.ensureNoLegacyDeployments(ctx, c, cr)).To(Succeed())
	})

	It("passes when no legacy Deployment exists", func() {
		Expect(extension.ensureNoLegacyDeployments(ctx, newFakeClient(), cr)).To(Succeed())
	})
})
