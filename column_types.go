package snowflake

import (
	"reflect"
	"strings"

	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
)

func (m Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	cols, err := m.Migrator.ColumnTypes(value)
	if err != nil {
		return nil, err
	}

	normalized := make([]gorm.ColumnType, 0, len(cols))
	for _, col := range cols {
		normalized = append(normalized, &normalizedColumnType{c: col})
	}

	return normalized, nil
}

type normalizedColumnType struct {
	c gorm.ColumnType
}

func (n *normalizedColumnType) Name() string {
	return n.c.Name()
}

func (n *normalizedColumnType) DecimalSize() (precision int64, scale int64, ok bool) {
	return n.c.DecimalSize()
}

func (n *normalizedColumnType) Length() (length int64, ok bool) {
	return n.c.Length()
}

func (n *normalizedColumnType) Nullable() (nullable bool, ok bool) {
	return n.c.Nullable()
}

func (n *normalizedColumnType) DefaultValue() (value string, ok bool) {
	return n.c.DefaultValue()
}

func (n *normalizedColumnType) Comment() (value string, ok bool) {
	return n.c.Comment()
}

func (n *normalizedColumnType) PrimaryKey() (isPrimaryKey bool, ok bool) {
	return n.c.PrimaryKey()
}

func (n *normalizedColumnType) AutoIncrement() (isAutoIncrement bool, ok bool) {
	return n.c.AutoIncrement()
}

func (n *normalizedColumnType) Unique() (unique bool, ok bool) {
	return n.c.Unique()
}

func (n *normalizedColumnType) ColumnType() (columnType string, ok bool) {
	return n.c.ColumnType()
}

func (n *normalizedColumnType) ScanType() reflect.Type {
	return n.c.ScanType()
}

func (n *normalizedColumnType) DatabaseTypeName() string {
	raw := n.c.DatabaseTypeName()
	switch strings.ToUpper(raw) {
	case "NUMBER", "DECIMAL", "NUMERIC", "FIXED":
		if _, scale, ok := n.c.DecimalSize(); ok && scale > 0 {
			return "FLOAT"
		}
		return "BIGINT"
	case "TEXT", "VARCHAR":
		return "TEXT"
	case "BOOLEAN":
		return "BOOLEAN"
	case "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "DATE", "TIME":
		return raw
	default:
		log.Error().Str("raw", raw).Msg("snowflake DatabaseTypeName switch default for type:")
		return raw
	}
}
