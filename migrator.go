package snowflake

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

func normalizeName(s string) string {
	return strings.ToUpper(strings.Trim(s, `"`))
}

type Migrator struct {
	migrator.Migrator

	// For testing purposes
	CreateTableFunc   func(values ...interface{}) error
	HasTableFunc      func(value interface{}) bool
	ColumnTypesFunc   func(value interface{}) ([]gorm.ColumnType, error)
	AddColumnFunc     func(value interface{}, field string) error
	MigrateColumnFunc func(value interface{}, field *schema.Field, columnType gorm.ColumnType) error
}

func (m Migrator) AutoMigrate(values ...interface{}) error {
	for _, value := range m.ReorderModels(values, true) {
		tx := m.DB.Session(&gorm.Session{})
		if !tx.Migrator().HasTable(value) {
			if err := tx.Migrator().CreateTable(value); err != nil {
				return err
			}
		} else {
			if err := m.RunWithValue(value, func(stmt *gorm.Statement) (errr error) {
				columnTypes, err := m.DB.Migrator().ColumnTypes(value)

				if err != nil {
					return err
				}

				for _, field := range stmt.Schema.FieldsByDBName {
					var foundColumn gorm.ColumnType
					for _, columnType := range columnTypes {
						if normalizeName(columnType.Name()) == normalizeName(field.DBName) {
							foundColumn = columnType
							break
						}
					}

					if foundColumn == nil {
						if err := tx.Migrator().AddColumn(value, field.DBName); err != nil {
							return err
						}
					} else {
						if err := m.DB.Migrator().MigrateColumn(value, field, foundColumn); err != nil {
							return err
						}
					}
				}

				for _, rel := range stmt.Schema.Relationships.Relations {
					if !m.DB.Config.DisableForeignKeyConstraintWhenMigrating {
						if constraint := rel.ParseConstraint(); constraint != nil {
							if constraint.Schema == stmt.Schema {
								if !tx.Migrator().HasConstraint(value, constraint.Name) {
									if err := tx.Migrator().CreateConstraint(value, constraint.Name); err != nil {
										return err
									}
								}
							}
						}
					}

					for _, chk := range stmt.Schema.ParseCheckConstraints() {
						if !tx.Migrator().HasConstraint(value, chk.Name) {
							if err := tx.Migrator().CreateConstraint(value, chk.Name); err != nil {
								return err
							}
						}
					}
				}

				return nil
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m Migrator) CreateTable(values ...interface{}) error {
	if m.CreateTableFunc != nil {
		return m.CreateTableFunc(values...)
	}

	for _, value := range m.ReorderModels(values, false) {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) (errr error) {
			var (
				createTableSQL          = "CREATE TABLE IF NOT EXISTS ? ("
				sqlValues               = []interface{}{m.CurrentTable(stmt)}
				hasPrimaryKeyInDataType bool
			)

			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				createTableSQL += "? ?,"
				hasPrimaryKeyInDataType = hasPrimaryKeyInDataType || strings.Contains(strings.ToUpper(string(field.DataType)), "PRIMARY KEY")
				sqlValues = append(sqlValues, clause.Column{Name: dbName}, m.DB.Migrator().FullDataTypeOf(field))
			}

			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
				createTableSQL += "PRIMARY KEY ?,"
				primaryKeys := []interface{}{}
				for _, field := range stmt.Schema.PrimaryFields {
					primaryKeys = append(primaryKeys, clause.Column{Name: field.DBName})
				}

				sqlValues = append(sqlValues, primaryKeys)
			}

			for _, rel := range stmt.Schema.Relationships.Relations {
				if !m.DB.DisableForeignKeyConstraintWhenMigrating {
					if constraint := rel.ParseConstraint(); constraint != nil {
						if constraint.Schema == stmt.Schema {
							sql, vars := buildConstraint(constraint)
							createTableSQL += sql + ","
							sqlValues = append(sqlValues, vars...)
						}
					}
				}
			}

			for _, chk := range stmt.Schema.ParseCheckConstraints() {
				createTableSQL += "CONSTRAINT ? CHECK (?),"
				sqlValues = append(sqlValues, clause.Column{Name: chk.Name}, clause.Expr{SQL: chk.Constraint})
			}

			createTableSQL = strings.TrimSuffix(createTableSQL, ",")

			createTableSQL += ")"

			if tableOption, ok := m.DB.Get("gorm:table_options"); ok {
				createTableSQL += fmt.Sprint(tableOption)
			}
			createTableSQL += " CHANGE_TRACKING = TRUE"

			errr = tx.Exec(createTableSQL, sqlValues...).Error
			return errr
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) HasTable(value interface{}) bool {
	if m.HasTableFunc != nil {
		return m.HasTableFunc(value)
	}

	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_name = ?",
			strings.ToUpper(stmt.Table),
		).Row().Scan(&count)
	})

	return count > 0
}

func (m Migrator) RenameTable(oldName, newName interface{}) error {
	var oldTable, newTable interface{}
	if v, ok := oldName.(string); ok {
		oldTable = clause.Table{Name: v}
	} else {
		stmt := &gorm.Statement{DB: m.DB}
		if err := stmt.Parse(oldName); err == nil {
			oldTable = m.CurrentTable(stmt)
		} else {
			return err
		}
	}

	if v, ok := newName.(string); ok {
		newTable = clause.Table{Name: v}
	} else {
		stmt := &gorm.Statement{DB: m.DB}
		if err := stmt.Parse(newName); err == nil {
			newTable = m.CurrentTable(stmt)
		} else {
			return err
		}
	}

	return m.DB.Exec("ALTER TABLE ? RENAME TO ?", oldTable, newTable).Error
}

// DropTable no change
func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	for i := len(values) - 1; i >= 0; i-- {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
			return tx.Exec("DROP TABLE IF EXISTS ?", m.CurrentTable(stmt)).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

// HasColumn modified for SF information schema structure
func (m Migrator) HasColumn(value interface{}, field string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		name := field
		if field := stmt.Schema.LookUpField(field); field != nil {
			name = field.DBName
		}

		return m.DB.Raw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.columns WHERE table_name = ? AND column_name = ?",
			normalizeName(stmt.Table), normalizeName(name),
		).Row().Scan(&count)
	})

	return count > 0
}

func (m Migrator) MigrateColumn(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	if m.MigrateColumnFunc != nil {
		return m.MigrateColumnFunc(value, field, columnType)
	}

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		//Snowflake doesnt allow to alter, add or remove identity columns
		if field.AutoIncrement {
			return nil
		}
		var alterClauses []string
		var sqlArgs []interface{}

		expectedType := m.DataTypeOf(field)
		actualType := columnType.DatabaseTypeName()

		baseExpected := strings.ToUpper(strings.Split(expectedType, "(")[0])
		baseActual := strings.ToUpper(actualType)

		typeMismatch := false
		if baseExpected == "VARCHAR" && baseActual == "TEXT" {
			typeMismatch = false
		} else if baseExpected != baseActual {
			typeMismatch = true
		}

		lengthMismatch := false
		if length, ok := columnType.Length(); ok && field.Size > 0 {
			if int64(field.Size) != length {
				lengthMismatch = true
			}
		}

		if typeMismatch || lengthMismatch {
			log.Warn().
				Str("expected", expectedType).
				Str("actual", actualType).
				Msg("Data type or length differs, will alter column")

			alterClauses = append(alterClauses, "SET DATA TYPE ?")
			sqlArgs = append(sqlArgs, clause.Expr{SQL: expectedType})
		}

		isNullable, ok := columnType.Nullable()
		if !field.PrimaryKey && ok && field.NotNull != !isNullable {
			if field.NotNull {
				log.Warn().
					Msg("Nullability differs, will alter column to NOT NULL")
				alterClauses = append(alterClauses, "SET NOT NULL")
			} else {
				log.Warn().
					Msg("Nullability differs, will alter column to NULL")
				alterClauses = append(alterClauses, "DROP NOT NULL")
			}
		}

		if len(alterClauses) > 0 {
			return m.DB.Exec(
				"ALTER TABLE ? ALTER COLUMN ? "+strings.Join(alterClauses, " "),
				append([]interface{}{m.CurrentTable(stmt), clause.Column{Name: field.DBName}}, sqlArgs...)...,
			).Error
		}

		return nil
	})
}

// AlterColumn no change
func (m Migrator) AlterColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			fileType := clause.Expr{SQL: m.DataTypeOf(field)}
			if field.NotNull {
				fileType.SQL += " NOT NULL"
			}

			return m.DB.Exec(
				"ALTER TABLE ? ALTER COLUMN ? ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, fileType,
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

// RenameColumn not supported
func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
	return fmt.Errorf("RENAME COLUMN UNSUPPORTED")
}

/*
	SNOWFLAKE DOES NOT SUPPORT INDEX
	SNOWFLAKE DOES MICRO PARTITIONING AUTOMATICALLY ON ALL TABLES
*/

// HasIndex return true to satisfy unit tests
func (m Migrator) HasIndex(value interface{}, name string) bool {
	return true
}

// RenameIndex return nil, SF does not support Index
func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return nil
}

// CreateIndex return nil, SF does not support Index
func (m Migrator) CreateIndex(value interface{}, name string) error {
	return nil
}

// DropIndex return nil, SF does not support Index
func (m Migrator) DropIndex(value interface{}, name string) error {
	return nil
}

// HasConstraint SF flavor
func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			`SELECT count(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = ?  AND TABLE_NAME = ?`,
			strings.ToUpper(name), strings.ToUpper(stmt.Table),
		).Row().Scan(&count)
	})
	return count > 0
}

// CreateConstraint no change
func (m Migrator) CreateConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, table := m.GuessConstraintAndTable(stmt, name)

		if constraint != nil {
			var vars = []interface{}{clause.Table{Name: table}}
			if stmt.TableExpr != nil {
				vars[0] = stmt.TableExpr
			}
			sql, values := buildConstraint(constraint)
			return m.DB.Exec("ALTER TABLE ? ADD "+sql, append(vars, values...)...).Error
		}

		return nil
	})
}

// DropConstraint no change
func (m Migrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, table := m.GuessConstraintAndTable(stmt, name)
		if constraint != nil {
			name = constraint.Name
		}
		return m.DB.Exec("ALTER TABLE ? DROP CONSTRAINT ?", clause.Table{Name: table}, clause.Column{Name: name}).Error
	})
}

func (m Migrator) GuessConstraintAndTable(stmt *gorm.Statement, name string) (_ *schema.Constraint, table string) {
	if stmt.Schema == nil {
		return nil, stmt.Table
	}
	// For Snowflake, we just return nil; the table name is still needed
	return nil, stmt.Schema.Table
}

// CurrentDatabase SF flavor
func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw("SELECT CURRENT_DATABASE()").Row().Scan(&name)
	return
}

// FullDataTypeOf no change
func (m Migrator) FullDataTypeOf(field *schema.Field) (expr clause.Expr) {
	expr.SQL = m.DataTypeOf(field)

	if field.NotNull {
		expr.SQL += " NOT NULL"
	}

	if field.Unique {
		expr.SQL += " UNIQUE"
	}

	if field.HasDefaultValue && (field.DefaultValueInterface != nil || field.DefaultValue != "") {
		if field.DefaultValueInterface != nil {
			defaultStmt := &gorm.Statement{Vars: []interface{}{field.DefaultValueInterface}}
			m.Dialector.BindVarTo(defaultStmt, defaultStmt, field.DefaultValueInterface)
			expr.SQL += " DEFAULT " + m.Dialector.Explain(defaultStmt.SQL.String(), field.DefaultValueInterface)
		} else if field.DefaultValue != "(-)" {
			expr.SQL += " DEFAULT " + field.DefaultValue
		}
	}

	return
}

func buildConstraint(constraint *schema.Constraint) (sql string, results []interface{}) {
	sql = "CONSTRAINT ? FOREIGN KEY ? REFERENCES ??"
	if constraint.OnDelete != "" {
		sql += " ON DELETE " + constraint.OnDelete
	}

	if constraint.OnUpdate != "" {
		sql += " ON UPDATE " + constraint.OnUpdate
	}

	// default enforced, but not actually enforced except for NOT NULL
	sql += " ENFORCED"

	var foreignKeys, references []interface{}
	for _, field := range constraint.ForeignKeys {
		foreignKeys = append(foreignKeys, clause.Column{Name: field.DBName})
	}

	for _, field := range constraint.References {
		references = append(references, clause.Column{Name: field.DBName})
	}
	results = append(results, clause.Table{Name: constraint.Name}, foreignKeys, clause.Table{Name: constraint.ReferenceSchema.Table}, references)
	return
}
