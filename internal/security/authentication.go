package security

import (
	"context"
	"maps"
	"slices"

	"emperror.dev/errors"
	dolphinv1alpha1 "github.com/zncdatadev/dolphinscheduler-operator/api/v1alpha1"
	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	opgosecurity "github.com/zncdatadev/operator-go/pkg/security"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var authenticationLogger = ctrl.Log.WithName("authentication-log")

const (
	// DEFAULT_OIDC_PROVIDER is the assumed OIDC provider if no hint is given in the AuthClass
	DEFAULT_OIDC_PROVIDER OIDCIdentityProvierHit = Keycloak
)

const (
	RedirectUri = "http://127.0.0.1:12345/dolphinscheduler/redirect/login/oauth2"
	CallbackUri = "http://127.0.0.1:12345/dolphinscheduler/ui/login"

	OidcClientIdKey = "CLIENT_ID"
	OidcSecretKey   = "CLIENT_SECRET"
)

var (
	SUPPORTED_AUTHENTICATION_CLASS_PROVIDERS = []AuthenticationType{LDAP, OIDC}
	SUPPORTED_OIDC_PROVIDERS                 = []OIDCIdentityProvierHit{Github}
)

// AuthenticationResult is the resolved authentication contribution to the api-server container.
type AuthenticationResult struct {
	// EnvVars are the security env vars, sorted by name (the legacy SortedMap rendering).
	EnvVars []corev1.EnvVar

	// LdapExportCommand, when non-empty, is the script prologue exporting the LDAP bind
	// credentials from the mounted secret files.
	LdapExportCommand string

	// Provisioner supplies the LDAP bind-credentials CSI volume + mount; nil without LDAP.
	Provisioner *opgosecurity.SecretProvisioner
}

// Authentication generates the authentication configuration for the Scheduler.
// It resolves the AuthenticationClass and based on the provider in the
// AuthenticationClass, it generates the configuration for the Scheduler.
// Supported providers are LDAP and OIDC.
func Authentication(
	ctx context.Context,
	c ctrlclient.Client,
	authSpec []dolphinv1alpha1.AuthenticationSpec,
) (*AuthenticationResult, error) {
	providers, err := resolveAuthentications(ctx, c, authSpec)
	if err != nil {
		// Legacy behavior: a resolution failure is logged and the remaining providers apply.
		authenticationLogger.Error(err, "Failed to resolve AuthenticationClass")
	}

	config, err := createAuthenticationConfig(providers)
	if err != nil {
		authenticationLogger.Error(err, "Failed to create AuthenticationConfig")
		return nil, err
	}

	result := &AuthenticationResult{
		EnvVars: sortedEnvVars(config),
	}

	for _, provider := range providers {
		if provider.AuthType == LDAP {
			if provider.Provider.LDAP == nil || provider.Provider.LDAP.BindCredentials == nil {
				return nil, errors.New("ldap provider or bind credentials cannot be nil")
			}
			result.Provisioner = LdapBindCredentialsProvisioner(*provider.Provider.LDAP.BindCredentials)
			result.LdapExportCommand = ExtractLdapCredintialsAndExportCommand()
			break
		}
	}

	return result, nil
}

// sortedEnvVars converts the generated security config (string values, or corev1.EnvVarSource
// for secret-backed values) into an env var list sorted by name — the legacy SortedMap order.
func sortedEnvVars(config map[string]interface{}) []corev1.EnvVar {
	envs := make([]corev1.EnvVar, 0, len(config))
	for _, name := range slices.Sorted(maps.Keys(config)) {
		switch value := config[name].(type) {
		case string:
			envs = append(envs, corev1.EnvVar{Name: name, Value: value})
		case corev1.EnvVarSource:
			source := value
			envs = append(envs, corev1.EnvVar{Name: name, ValueFrom: &source})
		}
	}
	return envs
}

func createAuthenticationConfig(providers []AuthenticationProvider) (config map[string]interface{}, err error) {
	var authenticationConfigGenerator AuthenticationConfigGenerator
	config = make(map[string]interface{})
	ldapExists := false // ldap resolve the first allways
	for _, provider := range providers {
		authType := provider.AuthType
		switch authType {
		case OIDC:
			authenticationConfigGenerator = NewOidcAuthenticationConfigGenerator(&provider)
		case LDAP:
			if !ldapExists {
				authenticationConfigGenerator = NewLDAPAuthenticationConfigGenerator(provider.Provider.LDAP)
				ldapExists = true
			} else {
				continue
			}
		default:
			err = errors.NewWithDetails("auth type is not supported", "authentication type", authType)
			return config, err
		}
		var providerHintSecurityConfig map[string]interface{}
		providerHintSecurityConfig, err = authenticationConfigGenerator.Generate()
		if err != nil {
			return config, err
		}
		maps.Copy(config, providerHintSecurityConfig)
	}
	return config, err
}

func resolveAuthentications(
	ctx context.Context,
	c ctrlclient.Client,
	anthenticantions []dolphinv1alpha1.AuthenticationSpec,
) (providers []AuthenticationProvider, err error) {
	for _, dolphinAuth := range anthenticantions {
		var authclass *authv1alpha1.AuthenticationClass
		if authclass, err = resolveAuthenticationClass(ctx, c, dolphinAuth.AuthenticationClass); err == nil {
			var authprovider *AuthenticationProvider
			authprovider, err = getAuthenticationProvider(authclass, dolphinAuth.Oidc)
			if err != nil {
				return
			}
			authType := authprovider.AuthType
			if isAuthenticationSupported(authType) {
				providers = append(providers, *authprovider)
			} else {
				err = errors.NewWithDetails("auth type is not supported", "actual type", authType)
				return
			}
		}
	}
	return
}

func resolveAuthenticationClass(
	ctx context.Context,
	c ctrlclient.Client,
	authClassRef string,
) (*authv1alpha1.AuthenticationClass, error) {
	// AuthenticationClass is cluster-scoped: no namespace in the lookup key.
	authClassObject := &authv1alpha1.AuthenticationClass{}
	if err := c.Get(ctx, types.NamespacedName{Name: authClassRef}, authClassObject); err != nil {
		authenticationLogger.Error(err, "Failed to get AuthenticationClass", "authClass ref", authClassRef)
		return nil, err
	}
	return authClassObject, nil
}

func getAuthenticationProvider(
	authClass *authv1alpha1.AuthenticationClass,
	oidcSecretSpec *dolphinv1alpha1.OidcCredentialSecretSpec) (authProvider *AuthenticationProvider, err error) {
	if authClass == nil {
		err = errors.New("AuthenticationClass cannot be nil")
		return
	}
	provider := authClass.Spec.AuthenticationProvider
	if provider == nil {
		err = errors.New("AuthenticationProvider cannot be nil")
		return
	}
	switch {
	case provider.OIDC != nil:
		var providerHint OIDCIdentityProvierHit
		providerHint, err = getOidcProviderHint(provider.OIDC)
		if err != nil {
			return
		}
		authProvider = NewOidcProvider(OIDC, providerHint, oidcSecretSpec, provider)
	case provider.TLS != nil:
		err = errors.New("TLS authentication provider is not supported")
	case provider.Static != nil:
		err = errors.New("static authentication provider is not supported")
	case provider.LDAP != nil:
		authProvider = NewLdapProvider(LDAP, provider)
	default:
		err = errors.New("no supported authentication provider is configured")
	}
	return
}

func getOidcProviderHint(oidcProvider *authv1alpha1.OIDCProvider) (hint OIDCIdentityProvierHit, err error) {
	switch oidcProvider.ProviderHint {
	case "keycloak":
		hint = Keycloak
	case "github":
		hint = Github
	case "oidc":
		hint = Github // todo:for test only
	default:
		err = errors.NewWithDetails("oidc provider hint is not supported", "actual provider hint", oidcProvider.ProviderHint)
	}
	return
}

func isAuthenticationSupported(authType AuthenticationType) bool {
	return slices.Contains(SUPPORTED_AUTHENTICATION_CLASS_PROVIDERS, authType)
}
