package util

import (
	"context"
	"strings"

	"emperror.dev/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ConnectionStringAuthUser = "username"
	ConnectionStringAuthPass = "password"
)

type DatabaseConfig struct {
	DbType   string
	Driver   string
	Username string
	Password string
	Host     string
	Port     string
	DbName   string
}

func NewDataBaseExtractor(client ctrlclient.Client, connectionString *string) *DataBaseExtractor {
	return &DataBaseExtractor{
		ConnectionString: connectionString,
		Client:           client,
	}
}

type DataBaseExtractor struct {
	ConnectionString    *string
	SecretReferenceName *string
	Namespace           *string
	Client              ctrlclient.Client
}

func (d *DataBaseExtractor) CredentialsInSecret(secretRefName string, namespace string) *DataBaseExtractor {
	d.SecretReferenceName = &secretRefName
	d.Namespace = &namespace
	return d
}

func (d *DataBaseExtractor) ExtractDatabaseInfo(ctx context.Context) (*DatabaseConfig, error) {
	if d.ConnectionString == nil {
		return nil, errors.New("connection string is empty")
	}

	dbInfo, err := d.parseConnectionString()
	if err != nil {
		return nil, err
	}

	if err := d.setCredentials(ctx, dbInfo); err != nil {
		return nil, err
	}

	return dbInfo, nil
}

func (d *DataBaseExtractor) parseConnectionString() (*DatabaseConfig, error) {
	parts := strings.Split(*d.ConnectionString, ":")
	if len(parts) < 3 {
		return nil, errors.New("invalid connection string")
	}

	dbInfo := &DatabaseConfig{}
	dbInfo.DbType = parts[1]

	driver, err := GetDatabaseDriver(dbInfo.DbType)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get database driver")
	}
	dbInfo.Driver = driver

	hostParts := strings.Split(parts[2], "/")
	if len(hostParts) < 2 {
		return nil, errors.New("invalid connection string")
	}
	dbInfo.Host = hostParts[2]

	portParts := strings.Split(parts[3], "/")
	dbInfo.Port = portParts[0]

	dbParts := strings.Split(portParts[1], "?")
	dbInfo.DbName = dbParts[0]

	return dbInfo, nil
}

func (d *DataBaseExtractor) setCredentials(ctx context.Context, dbInfo *DatabaseConfig) error {
	if d.SecretReferenceName != nil {
		return d.setCredentialsFromSecret(ctx, dbInfo)
	}
	return d.setCredentialsFromConnectionString(dbInfo)
}

func (d *DataBaseExtractor) setCredentialsFromSecret(ctx context.Context, dbInfo *DatabaseConfig) error {
	secret := &corev1.Secret{}

	details := map[string]interface{}{
		"namespace":   d.Namespace,
		"secret_name": *d.SecretReferenceName,
	}

	key := types.NamespacedName{Namespace: *d.Namespace, Name: *d.SecretReferenceName}
	if err := d.Client.Get(ctx, key, secret); err != nil {
		return errors.WrapWithDetails(err, "failed to get database secret", details)
	}

	secretData := secret.Data
	if username, ok := secretData[ConnectionStringAuthUser]; ok {
		dbInfo.Username = string(username)
	} else {
		return errors.NewWithDetails("database username is empty", details)
	}

	if password, ok := secretData[ConnectionStringAuthPass]; ok {
		dbInfo.Password = string(password)
	} else {
		return errors.NewWithDetails("database password is empty", details)
	}

	return nil
}

func (d *DataBaseExtractor) setCredentialsFromConnectionString(dbInfo *DatabaseConfig) error {
	parts := strings.Split(*d.ConnectionString, "?")
	if len(parts) < 2 {
		return errors.New("database credential is empty")
	}

	query := strings.Split(parts[1], "&")
	for _, q := range query {
		kv := strings.Split(q, "=")
		if len(kv) != 2 {
			continue
		}
		if kv[0] == "user" || kv[0] == "username" {
			dbInfo.Username = kv[1]
		}
		if kv[0] == ConnectionStringAuthPass {
			dbInfo.Password = kv[1]
		}
	}

	return nil
}

// get database driver by database type
func GetDatabaseDriver(dbType string) (string, error) {
	switch dbType {
	case "mysql":
		return "com.mysql.cj.jdbc.Driver", nil
	case "postgresql":
		return "org.postgresql.Driver", nil
	default:
		return "", errors.NewWithDetails("unsupported database type to get driver class",
			map[string]interface{}{"type": dbType})
	}
}

func GetDatabaseHost(connectionString string) (host string, port string) {
	// eg. jdbc:postgresql://127.0.0.1:5432/dolphinscheduler?user=root&password=root
	hostPort := strings.Split(connectionString, "//")[1]
	hostPort = strings.Split(hostPort, "/")[0]
	hosts := strings.Split(hostPort, ":")
	if len(hosts) == 2 {
		host = hosts[0]
		port = hosts[1]
	}
	return
}
