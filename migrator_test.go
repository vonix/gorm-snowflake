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
	var addColumnCallCount int

	mockCreateTable := func(values ...interface{}) error {
		createTableCalled = true
		return nil
	}

	mockHasTable := func(value interface{}) bool {
		hasTableCalled = true
		return true
	}

	mockAddColumn := func(value interface{}, field string) error {
		addColumnCallCount++
		return nil
	}

	mockColumnTypes := func(value interface{}) ([]gorm.ColumnType, error) {
		emptySliceNoColumns := []gorm.ColumnType{}
		return emptySliceNoColumns, nil
	}

	mockDb, _, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDb.Close()

	dialector := snowflake.New(snowflake.Config{
		CreateTableFunc: mockCreateTable,
		HasTableFunc:    mockHasTable,
		AddColumnFunc:   mockAddColumn,
		ColumnTypesFunc: mockColumnTypes,
		Conn:            mockDb,
	})

	db, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)

	err = db.AutoMigrate(&User{})
	require.NoError(t, err)

	require.True(t, hasTableCalled, "HasTable should have been called")
	require.False(t, createTableCalled, "CreateTable should not have been called because HasTable returned true")
	require.Equal(t, 2, addColumnCallCount, "AddColumn should be called for ID and Name fields")
}

func TestHasColumn_GeneratesCorrectSQL(t *testing.T) {
	mockDb, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDb.Close()

	dialector := snowflake.New(snowflake.Config{
		Conn: mockDb,
	})

	db, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)

	mock.ExpectQuery(`SELECT count\(\*\) FROM INFORMATION_SCHEMA.columns WHERE table_name = \? AND column_name = \?`).
		WithArgs("USERS", "NAME").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	has := db.Migrator().HasColumn(&User{}, "Name")
	require.True(t, has)
}
