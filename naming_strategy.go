package snowflake

import (
	"crypto/sha1"
	"encoding/hex"
	"math"
	"strings"

	"gorm.io/gorm/schema"
)

type NamingStrategy struct {
	defaultNS schema.Namer
}

func NewNamingStrategy() schema.Namer {
	return &NamingStrategy{
		defaultNS: schema.NamingStrategy{},
	}
}

const maxIdentifierLength = 255

func truncateWithHash(name string) string {
	name = strings.ToUpper(name)

	if len(name) <= maxIdentifierLength {
		return name
	}
	sum := sha1.Sum([]byte(name))
	tail := hex.EncodeToString(sum[:8])
	keep := int(math.Max(float64(maxIdentifierLength-1-len(tail)), 1))

	return name[:keep] + "_" + tail
}

func (sns NamingStrategy) ColumnName(table, column string) string {
	column = strings.ToUpper(sns.defaultNS.ColumnName(table, column))
	reserved := map[string]struct{}{
		"ORDER": {}, "LOCALITY": {},
	}

	if _, isReserved := reserved[column]; isReserved {
		return `"` + column + `"`
	}
	return column
}

func (sns NamingStrategy) TableName(table string) string {
	return strings.ToUpper(sns.defaultNS.TableName(table))
}

func (sns NamingStrategy) SchemaName(table string) string {
	return strings.ToUpper(sns.defaultNS.SchemaName(table))
}

func (sns NamingStrategy) JoinTableName(joinTable string) string {
	return strings.ToUpper(sns.defaultNS.JoinTableName(joinTable))
}

func (sns NamingStrategy) RelationshipFKName(rel schema.Relationship) string {
	name := sns.defaultNS.RelationshipFKName(rel)
	return truncateWithHash(name)
}

func (sns NamingStrategy) CheckerName(table, column string) string {
	name := sns.defaultNS.CheckerName(table, column)
	return truncateWithHash(name)
}

func (sns NamingStrategy) IndexName(table, column string) string {
	name := sns.defaultNS.IndexName(table, column)
	return truncateWithHash(name)
}

func (sns NamingStrategy) UniqueName(table, column string) string {
	base := table + "_" + column

	return truncateWithHash(base)
}
