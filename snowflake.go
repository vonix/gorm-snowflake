package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/snowflakedb/gosnowflake"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

const (
	SnowflakeDriverName = "snowflake"
)

var (
	ErrInvalidPEMFormat     = errors.New("invalid PEM format: private key must be in valid PEM format")
	ErrMissingRequiredField = errors.New("missing required field for key-pair authentication")
	ErrKeyParsingFailed     = errors.New("failed to parse private key")
	ErrEmptyPrivateKey      = errors.New("private key cannot be empty")
	ErrUnsupportedKeyType   = errors.New("unsupported private key type: only RSA keys are supported")
	ErrKeyValidationFailed  = errors.New("private key validation failed")

	ErrInvalidAccount   = errors.New("invalid account: account name cannot be empty or contain invalid characters")
	ErrInvalidUser      = errors.New("invalid user: username cannot be empty")
	ErrInvalidDatabase  = errors.New("invalid database: database name cannot be empty")
	ErrConnectionFailed = errors.New("failed to establish connection with key-pair authentication")

	ErrMalformedPEMBlock   = errors.New("malformed PEM block: no valid PEM data found")
	ErrInvalidPEMBlockType = errors.New("invalid PEM block type: expected PRIVATE KEY or RSA PRIVATE KEY")
)

type Dialector struct {
	*Config
}

type Config struct {
	DriverName string
	DSN        string
	Conn       gorm.ConnPool
	Connector  driver.Connector //connector support for key-pair auth

	// For testing purposes
	CreateTableFunc   func(values ...interface{}) error
	HasTableFunc      func(value interface{}) bool
	ColumnTypesFunc   func(value interface{}) ([]gorm.ColumnType, error)
	AddColumnFunc     func(value interface{}, field string) error
	MigrateColumnFunc func(value interface{}, field *schema.Field, columnType gorm.ColumnType) error
}

func (dialector Dialector) Name() string {
	return SnowflakeDriverName
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{
		Config: &Config{
			DSN:        dsn,
			DriverName: SnowflakeDriverName,
		},
	}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func OpenWithKey(account, user, privateKeyPEM, database, schema, warehouse, role string) (gorm.Dialector, error) {
	if err := validateConnectionParameters(account, user, privateKeyPEM, database); err != nil {
		return nil, err
	}

	privateKey, err := parsePEMPrivateKey(privateKeyPEM)
	if err != nil {
		return nil, err
	}

	config := &gosnowflake.Config{
		Account:       account,
		User:          user,
		Database:      database,
		Schema:        schema,
		Warehouse:     warehouse,
		Role:          role,
		Authenticator: gosnowflake.AuthTypeJwt,
		PrivateKey:    privateKey,
	}

	connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *config)

	return &Dialector{
		Config: &Config{
			DriverName: SnowflakeDriverName,
			Connector:  connector,
		},
	}, nil
}

func validateConnectionParameters(account, user, privateKeyPEM, database string) error {
	if account == "" {
		return fmt.Errorf("%w: account is required", ErrInvalidAccount)
	}
	if strings.TrimSpace(account) == "" {
		return fmt.Errorf("%w: account cannot be only whitespace", ErrInvalidAccount)
	}

	if user == "" {
		return fmt.Errorf("%w: user is required", ErrInvalidUser)
	}
	if strings.TrimSpace(user) == "" {
		return fmt.Errorf("%w: user cannot be only whitespace", ErrInvalidUser)
	}

	if privateKeyPEM == "" {
		return fmt.Errorf("%w: privateKeyPEM is required", ErrEmptyPrivateKey)
	}
	if strings.TrimSpace(privateKeyPEM) == "" {
		return fmt.Errorf("%w: privateKeyPEM cannot be only whitespace", ErrEmptyPrivateKey)
	}

	if database == "" {
		return fmt.Errorf("%w: database is required", ErrInvalidDatabase)
	}
	if strings.TrimSpace(database) == "" {
		return fmt.Errorf("%w: database cannot be only whitespace", ErrInvalidDatabase)
	}

	return nil
}

func parsePEMPrivateKey(privateKeyPEM string) (*rsa.PrivateKey, error) {
	trimmedPEM := strings.TrimSpace(privateKeyPEM)
	if trimmedPEM == "" {
		return nil, fmt.Errorf("%w: private key string is empty after trimming whitespace", ErrEmptyPrivateKey)
	}

	if !strings.Contains(trimmedPEM, "-----BEGIN") || !strings.Contains(trimmedPEM, "-----END") {
		return nil, fmt.Errorf("%w: missing PEM header or footer markers", ErrMalformedPEMBlock)
	}

	block, _ := pem.Decode([]byte(trimmedPEM))
	if block == nil {
		return nil, fmt.Errorf("%w: no valid PEM block found in input", ErrMalformedPEMBlock)
	}

	if block.Type != "PRIVATE KEY" && block.Type != "RSA PRIVATE KEY" {
		return nil, fmt.Errorf("%w: expected 'PRIVATE KEY' or 'RSA PRIVATE KEY', got '%s'", ErrInvalidPEMBlockType, block.Type)
	}

	if len(block.Bytes) == 0 {
		return nil, fmt.Errorf("%w: PEM block contains no data", ErrMalformedPEMBlock)
	}

	var privateKey *rsa.PrivateKey
	var err error

	if block.Type == "PRIVATE KEY" {
		parsedKey, parseErr := x509.ParsePKCS8PrivateKey(block.Bytes)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: failed to parse PKCS#8 private key: %v", ErrKeyParsingFailed, parseErr)
		}

		var ok bool
		privateKey, ok = parsedKey.(*rsa.PrivateKey)
		if !ok {
			keyType := "unknown"
			switch parsedKey.(type) {
			case *rsa.PrivateKey:
				keyType = "RSA"
			default:
				keyType = fmt.Sprintf("%T", parsedKey)
			}
			return nil, fmt.Errorf("%w: found %s key, but only RSA keys are supported", ErrUnsupportedKeyType, keyType)
		}
	} else {
		privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to parse PKCS#1 RSA private key: %v", ErrKeyParsingFailed, err)
		}
	}

	if err := privateKey.Validate(); err != nil {
		return nil, fmt.Errorf("%w: RSA key structure validation failed: %v", ErrKeyValidationFailed, err)
	}

	keySize := privateKey.N.BitLen()
	if keySize < 2048 {
		return nil, fmt.Errorf("%w: RSA key size %d bits is too small, minimum 2048 bits required", ErrKeyValidationFailed, keySize)
	}

	return privateKey, nil
}

func (dialector Dialector) Initialize(db *gorm.DB) error {
	db.Config.NamingStrategy = NewNamingStrategy()
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})
	_ = db.Callback().Create().Replace("gorm:create", Create)

	dialector.DriverName = SnowflakeDriverName

	connPool, err := dialector.createConnectionPool()
	if err != nil {
		return err
	}

	db.ConnPool = connPool
	return nil
}

func (dialector Dialector) createConnectionPool() (gorm.ConnPool, error) {
	if dialector.Conn != nil {
		return dialector.Conn, nil
	}

	if dialector.Connector != nil {
		connPool := sql.OpenDB(dialector.Connector)
		if connPool == nil {
			return nil, fmt.Errorf("%w: failed to create connection pool with key-pair authentication", ErrConnectionFailed)
		}
		return connPool, nil
	}

	if dialector.DSN != "" {
		connPool, err := sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to open DSN connection: %w", err)
		}
		return connPool, nil
	}

	return nil, errors.New("no connection information provided: must specify either Conn, Connector, or DSN")
}

// func (dialector Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
// 	return map[string]clause.ClauseBuilder{
// 		"LIMIT": func(c clause.Clause, builder clause.Builder) {
// 			if limit, ok := c.Expression.(clause.Limit); ok {
// 				if stmt, ok := builder.(*gorm.Statement); ok {
// 					if _, ok := stmt.Clauses["ORDER BY"]; !ok {
// 						if stmt.Schema != nil && stmt.Schema.PrioritizedPrimaryField != nil {
// 							builder.WriteString("ORDER BY ")
// 							builder.WriteQuoted(stmt.Schema.PrioritizedPrimaryField.DBName)
// 							builder.WriteByte(' ')
// 						} else {
// 							builder.WriteString("ORDER BY (SELECT NULL) ")
// 						}
// 					}
// 				}

// 				if limit.Offset > 0 {
// 					builder.WriteString("OFFSET ")
// 					builder.WriteString(strconv.Itoa(limit.Offset))
// 					builder.WriteString(" ROWS")
// 				}

// 				if limit.Limit != nil && *limit.Limit > 0 {
// 					if limit.Offset == 0 {
// 						builder.WriteString("OFFSET 0 ROW")
// 					}
// 					builder.WriteString(" FETCH NEXT ")
// 					builder.WriteString(strconv.Itoa(*limit.Limit))
// 					builder.WriteString(" ROWS ONLY")
// 				}
// 			}
// 		},
// 	}
// }

func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	if field.AutoIncrement {
		return clause.Expr{SQL: "GENERATED BY DEFAULT AS IDENTITY"}
	}

	if field.DefaultValue == "" {
		return clause.Expr{SQL: "NULL"}
	}

	up := strings.ToUpper(field.DefaultValue)

	switch up {
	case "CURRENT_TIMESTAMP":
		return clause.Expr{SQL: "CURRENT_TIMESTAMP"}
	case "IDENTITY":
		return clause.Expr{SQL: "GENERATED BY DEFAULT AS IDENTITY"}
	default:
		if up == "TRUE" || up == "FALSE" {
			return clause.Expr{SQL: up}
		}

		if _, err := strconv.ParseFloat(field.DefaultValue, 64); err == nil {
			return clause.Expr{SQL: field.DefaultValue}
		}

		return clause.Expr{SQL: fmt.Sprintf("'%s'", field.DefaultValue)}
	}
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{Config: migrator.Config{
			DB:        db,
			Dialector: dialector,
		}},
		CreateTableFunc:   dialector.Config.CreateTableFunc,
		HasTableFunc:      dialector.Config.HasTableFunc,
		ColumnTypesFunc:   dialector.Config.ColumnTypesFunc,
		AddColumnFunc:     dialector.Config.AddColumnFunc,
		MigrateColumnFunc: dialector.Config.MigrateColumnFunc,
	}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteString(strings.ToUpper(str))
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int, schema.Uint:
		if field.AutoIncrement {
			return "BIGINT IDENTITY(1,1)"
		}
		return "BIGINT"
	case schema.Float:
		return "FLOAT"
	case schema.String:
		size := field.Size
		hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
		if (field.PrimaryKey || hasIndex) && size == 0 {
			size = 256
		}
		if size > 0 && size <= 4000 {
			return fmt.Sprintf("VARCHAR(%d)", size)
		}
		return "VARCHAR"
	case schema.Time:
		return "TIMESTAMP_NTZ"
	case schema.Bytes:
		return "VARBINARY"
	}

	return string(field.DataType)
}

// no support for savepoint
func (dialectopr Dialector) SavePoint(tx *gorm.DB, name string) error {
	return nil
}

func (dialectopr Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TRANSACTION " + name)
	return nil
}
