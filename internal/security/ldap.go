package security

import (
	"fmt"
	"path"

	dolphinv1alpha1 "github.com/zncdatadev/dolphinscheduler-operator/api/v1alpha1"
	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opgoconstant "github.com/zncdatadev/operator-go/pkg/constant"
	opgosecurity "github.com/zncdatadev/operator-go/pkg/security"
)

var _ AuthenticationConfigGenerator = &LDAPAuthenticationConfigGenerator{}

func NewLDAPAuthenticationConfigGenerator(ldap *authv1alpha1.LDAPProvider) *LDAPAuthenticationConfigGenerator {
	return &LDAPAuthenticationConfigGenerator{
		LDAPProvider: *ldap,
	}
}

type LDAPAuthenticationConfigGenerator struct {
	authv1alpha1.LDAPProvider
}

// Generate implements AuthenticationConfigGenerator.
func (l *LDAPAuthenticationConfigGenerator) Generate() (map[string]interface{}, error) {

	uidAttr := "uid"
	mailAttr := "mail"
	if l.LDAPFieldNames != nil {
		if l.LDAPFieldNames.Uid != "" {
			uidAttr = l.LDAPFieldNames.Uid
		}
		if l.LDAPFieldNames.Email != "" {
			mailAttr = l.LDAPFieldNames.Email
		}
	}

	ldapUrls := fmt.Sprintf("ldap://%s:%d", l.Hostname, l.Port)
	envKeyPrefix := "SECURITY_AUTHENTICATION_LDAP_"
	evns := map[string]interface{}{
		SecurityAuthenticationType: string(LDAP),
		// envKeyPrefix + "USER_ADMIN":              "read-only-admin", // export in command args
		envKeyPrefix + "URLS":                    ldapUrls,
		envKeyPrefix + "BASE-DN":                 l.SearchBase,
		envKeyPrefix + "USER_IDENTITY-ATTRIBUTE": uidAttr,
		envKeyPrefix + "USER_EMAIL-ATTRIBUTE":    mailAttr,
		envKeyPrefix + "USER_NOT-EXIST-ACTION":   "CREATE",
	}

	if l.TLS != nil {
		// WIP: support TLS
		evns[envKeyPrefix+"SSL_ENABLE"] = "true"
		evns[envKeyPrefix+"SSL_TRUST_STORE"] = path.Join(opgoconstant.KubedoopTlsDir, "truststore.p12")
		evns[envKeyPrefix+"SSL_TRUST_STORE_PASSWORD"] = ""
	}
	return evns, nil
}

// export ldap bind user and password by k8s-search
const (
	// security.authentication.type
	SecurityAuthenticationType = "SECURITY_AUTHENTICATION_TYPE"

	LdapBindCredintialsUser = "SECURITY_AUTHENTICATION_LDAP_USERNAME"
	LdapBindCredintialsPass = "SECURITY_AUTHENTICATION_LDAP_PASSWORD"
	LdapUserAdmin           = "SECURITY_AUTHENTICATION_LDAP_USER_ADMIN"

	LdapSecretUserKey = "user"
	LdapSecretPassKey = "password"

	// ldapBindCredentialsStorageSize is the legacy ephemeral secret volume storage request.
	ldapBindCredentialsStorageSize = "1Mi"
)

func ExtractLdapCredintialsAndExportCommand() string {
	// 1. Get ldap credentials from secret mount path
	// 2. Export ldap credentials to env
	userCredentialsSecret := path.Join(opgoconstant.KubedoopSecretDir, dolphinv1alpha1.LdapBindCredintialsVolumeName, LdapSecretUserKey)
	passCredentialsSecret := path.Join(opgoconstant.KubedoopSecretDir, dolphinv1alpha1.LdapBindCredintialsVolumeName, LdapSecretPassKey)
	cmd := fmt.Sprintf(`export SECURITY_AUTHENTICATION_LDAP_USERNAME="$(cat %s)";
export SECURITY_AUTHENTICATION_LDAP_PASSWORD="$(cat %s)";
export SECURITY_AUTHENTICATION_LDAP_USER_ADMIN="$(cat %s | grep -oP 'uid=\K[^,]+')";
echo "show ldap useranme and admin user: ldap-username: $SECURITY_AUTHENTICATION_LDAP_USERNAME, ldap-user-admin: $SECURITY_AUTHENTICATION_LDAP_USER_ADMIN "`, userCredentialsSecret, passCredentialsSecret, userCredentialsSecret)
	return cmd
}

// LdapBindCredentialsProvisioner builds the CSI secret provisioner supplying the LDAP
// bind-credentials volume: the legacy "ldap-bind-credentials" ephemeral volume (1Mi, class and
// scope annotations) mounted read-only at /kubedoop/secret/ldap-bind-credentials.
func LdapBindCredentialsProvisioner(bindCredentials commonsv1alpha1.Credentials) *opgosecurity.SecretProvisioner {
	registration := opgosecurity.CredentialsVolume(
		dolphinv1alpha1.LdapBindCredintialsVolumeName,
		bindCredentials.SecretClass,
	).WithStorageSize(ldapBindCredentialsStorageSize)

	// The legacy operator joined bare service names into the scope annotation, which the
	// secret-operator parser skips — a service-scoped bind credential silently lost its scope.
	// ScopeString emits the required "service=<name>" form.
	if scope := opgosecurity.ScopeString(bindCredentials.Scope); scope != "" {
		registration = registration.WithScope(scope)
	}

	return opgosecurity.NewSecretProvisioner().
		WithMountBasePath(opgoconstant.KubedoopSecretDir).
		Register(registration)
}
