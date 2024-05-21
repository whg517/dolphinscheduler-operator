package resource

import (
	"fmt"

	"github.com/zncdatadev/dolphinscheduler-operator/pkg/core"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	S3AccessKeyName = "ACCESS_KEY"
	S3SecretKeyName = "SECRET_KEY"
)

type S3Params struct {
	AccessKey string
	SecretKey string
	Endpoint  string
	Bucket    string
	Region    string
	SSL       bool
	PathStyle bool
}

type S3Configuration struct {
	S3Reference    *string
	S3Inline       *S3Params
	ResourceClient *core.ResourceClient
}

func (s *S3Configuration) GetRefBucketName() string {
	return *s.S3Reference
}

func (s *S3Configuration) GetRefBucket() (*commonsv1alpha1.S3Bucket, error) {
	s3BucketCR := &commonsv1alpha1.S3Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: s.ResourceClient.Namespace,
			Name:      s.GetRefBucketName(),
		},
	}
	// Get Commons S3Bucket CR from the reference
	if err := s.ResourceClient.Get(s3BucketCR); err != nil {
		return nil, err
	}
	return s3BucketCR, nil
}

func (s *S3Configuration) GetRefConnection(name string) (*commonsv1alpha1.S3Connection, error) {
	S3ConnectionCR := &commonsv1alpha1.S3Connection{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: s.ResourceClient.Namespace,
			Name:      name,
		},
	}
	if err := s.ResourceClient.Get(S3ConnectionCR); err != nil {
		return nil, err
	}
	return S3ConnectionCR, nil
}

type S3Credential struct {
	AccessKey string `json:"ACCESS_KEY"`
	SecretKey string `json:"SECRET_KEY"`
}

func (s *S3Configuration) GetCredential(name string) (*S3Credential, error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: s.ResourceClient.Namespace,
			Name:      name,
		},
	}
	if err := s.ResourceClient.Get(secret); err != nil {
		return nil, err
	}
	ak, err := util.Base64[[]byte]{Data: secret.Data[S3AccessKeyName]}.Decode()
	if err != nil {
		return nil, err
	}
	sk, err := util.Base64[[]byte]{Data: secret.Data[S3SecretKeyName]}.Decode()
	if err != nil {
		return nil, err
	}
	return &S3Credential{
		AccessKey: string(ak),
		SecretKey: string(sk),
	}, nil
}

func (s *S3Configuration) ExistingS3Bucket() bool {
	return s.S3Reference != nil
}

func (s *S3Configuration) GetS3Params() (*S3Params, error) {
	if s.S3Reference != nil {
		return s.GetS3ParamsFromResource()
	}
	if s.S3Inline != nil {
		return s.GetS3ParamsFromInline()
	}
	return nil, fmt.Errorf("invalid s3 configuration, s3Reference and s3Inline cannot be empty at the same time")
}

func (s *S3Configuration) GetS3ParamsFromResource() (*S3Params, error) {

	s3BucketCR, err := s.GetRefBucket()
	if err != nil {
		return nil, err
	}
	credential := &S3Credential{}

	if s3BucketCR.Spec.Credential.ExistSecret != "" {
		existCredential, err := s.GetCredential(s3BucketCR.Spec.Credential.ExistSecret)
		if err != nil {
			return nil, err
		}
		credential = existCredential
	} else {
		credential.AccessKey = s3BucketCR.Spec.Credential.AccessKey
		credential.SecretKey = s3BucketCR.Spec.Credential.SecretKey
	}

	s3ConnectionCR, err := s.GetRefConnection(s3BucketCR.Spec.Reference)
	if err != nil {
		return nil, err
	}
	return &S3Params{
		AccessKey: credential.AccessKey,
		SecretKey: credential.SecretKey,
		Endpoint:  s3ConnectionCR.Spec.Endpoint,
		Region:    s3ConnectionCR.Spec.Region,
		SSL:       s3ConnectionCR.Spec.SSL,
		PathStyle: s3ConnectionCR.Spec.PathStyle,
		Bucket:    s3BucketCR.Spec.BucketName,
	}, nil
}

func (s *S3Configuration) GetS3ParamsFromInline() (*S3Params, error) {
	return s.S3Inline, nil
}
