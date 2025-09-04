package snowflake_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	snowflake "github.com/Kinoo3/gorm-snowflake"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type User struct {
	ID   int64
	Name string
}

func TestAutoMigrate_CreatesTableWhenItDoesNotExist(t *testing.T) {
	var createTableCalled bool
	var hasTableCalled bool

	mockCreateTable := func(values ...interface{}) error {
		createTableCalled = true
		return nil
	}

	mockHasTable := func(value interface{}) bool {
		hasTableCalled = true
		return false
	}

	mockDb, _, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDb.Close()

	dialector := snowflake.New(snowflake.Config{
		CreateTableFunc: mockCreateTable,
		HasTableFunc:    mockHasTable,
		Conn:            mockDb,
	})

	db, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)

	err = db.AutoMigrate(&User{})
	require.NoError(t, err)

	require.True(t, hasTableCalled, "HasTable should have been called")
	require.True(t, createTableCalled, "CreateTable should have been called because HasTable returned false")
}
func TestAutoMigrate_DoesNotCreateTableWhenItExists(t *testing.T) {
	var createTableCalled bool
	var hasTableCalled bool

	mockCreateTable := func(values ...interface{}) error {
		createTableCalled = true
		return nil
	}

	mockHasTable := func(value interface{}) bool {
		hasTableCalled = true
		return true
	}

	mockDb, _, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDb.Close()

	dialector := snowflake.New(snowflake.Config{
		CreateTableFunc: mockCreateTable,
		HasTableFunc:    mockHasTable,
		Conn:            mockDb,
	})

	db, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)

	err = db.AutoMigrate(&User{})
	require.NoError(t, err)

	require.True(t, hasTableCalled, "HasTable should have been called")
	require.False(t, createTableCalled, "CreateTable should not have been called because HasTable returned true")
}

// func TestHasColumnSQL(t *testing.T) {
// 	db, err := gorm.Open(snowflake.Open("fake-dsn"), &gorm.Config{
// 		DryRun: true,
// 	})

// 	if err != nil {
// 		t.Errorf("error connecting to db")
// 	}
// 	// require.NoError(t, err)

// 	stmt := db.Session(&gorm.Session{DryRun: true}).
// 		Raw("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?", "CALLS", "STATUS_CAUSE").
// 		Statement

// 	sql := stmt.SQL.String()
// 	t.Errorf("expected NULL, got %s", sql)

// 	// require.Contains(t, strings.ToUpper(sql), "INFORMATION_SCHEMA.COLUMNS")
// 	// require.Contains(t, sql, "STATUS_CAUSE")
// }
