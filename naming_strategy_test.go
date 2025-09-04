package snowflake_test

import (
	"strings"
	"testing"

	snowflake "github.com/Kinoo3/gorm-snowflake"
)

func TestUniqueName_TruncationAndDeterminism(t *testing.T) {
    ns := snowflake.NewNamingStrategy()

    table := "THIS_IS_A_VERY_LONG_TABLE_NAME_THAT_EXCEEDS_THE_MAX_IDENTIFIER_LENGTH_OF_SNOWFLAKE_FOR_TESTING_PURPOSES"
    column := "COLUMN_WITH_AN_EQUALLY_LONG_NAME_TO_FORCE_HASH_TRUNCATION"

    name1 := ns.UniqueName(table, column)
    name2 := ns.UniqueName(table, column)

    if len(name1) > 255 {
        t.Errorf("UniqueName exceeds 255 characters: %d", len(name1))
    }

    if name1 != name2 {
        t.Errorf("UniqueName is not deterministic: %s != %s", name1, name2)
    }

	needsTruncation := len(table)+len(column)+1 > 255
    if needsTruncation && !strings.Contains(name1, "_") {
        t.Errorf("Expected truncated UniqueName to contain '_' separator, got %s", name1)
    }
}

func TestUniqueName_NoTruncation(t *testing.T) {
    ns := snowflake.NewNamingStrategy()
    got := ns.UniqueName("agents", "id")
    want := "AGENTS_ID"
    if got != want {
        t.Errorf("expected %s, got %s", want, got)
    }
}

func TestColumnName_ToUppercase(t *testing.T) {
    ns := snowflake.NewNamingStrategy()
    got := ns.ColumnName("users", "created_at")
    want := "CREATED_AT"
    if got != want {
        t.Errorf("expected %s, got %s", want, got)
    }
}

func TestColumnName_CamelCaseToUpperSnake(t *testing.T) {
    ns := snowflake.NewNamingStrategy()
    got := ns.ColumnName("users", "EmailAddress")
    want := "EMAIL_ADDRESS"
    if got != want {
        t.Errorf("expected %s, got %s", want, got)
    }
}

func TestColumnName_ReservedWordUppercase(t *testing.T) {
    ns := snowflake.NewNamingStrategy()
    got := ns.ColumnName("sessions", "user")
    if got != "USER" {
        t.Errorf("expected USER, got %s", got)
    }
}

func TestColumnName_NoQuotes(t *testing.T) {
    ns := snowflake.NewNamingStrategy()
    got := ns.ColumnName("users", "email")
    if strings.Contains(got, `"`) {
        t.Errorf("expected no quotes, got %s", got)
    }
}
