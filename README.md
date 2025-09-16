# gorm-snowflake
Snowflake driver for [gorm](https://gorm.io/)

## Snowflake Features

Notable Snowflake (SF) features that affect decisions in this driver

- Snowflake only enforces case-sensitivy with the use of quotes. Decision: treat all as uppercase and avoid treating case sensitivity at all.
- SF does not support INDEX, it does micro-partitioning automatically in all tables for optimizations. Therefore all Index related functions are nil-returned.
- Transactions in SF do not support SAVEPOINT (https://docs.snowflake.com/en/sql-reference/transactions.html)
- The `SELECT...CHANGES` feature of SF does not return unchanged rows from `MERGE` statement, therefore we can only rely on the `APPEND_ONLY` option and only support returning fields from inserted rows in the same order.
- SF does not enforce any constraint other than NOT NULL.
Currently disabled (commented out):
- GORM rely on being able to query back inserted rows in every transaction in order to get default values back.
 There is no easy way to do this ala SQL Server (`OUTPUT INSERTED`) or Postgres (`RETURNING`). Instead, we automatically turn on SF `CHANGE_TRACKING` feature on for all tables. This allows us to run `CHANGES` query on the table after running any DML. However due to non-deterministic nature of return from `MERGE`, it doesn't support updates.
## How To

How to use this project

```go
package main

import (
    "fmt"
    "log"

    snowflake "github.com/Kinoo3/gorm-snowflake"
    "gorm.io/gorm"
)

func main() {
    // Snowflake DSN format: user:password@account/database/schema?warehouse=WH&role=ROLE
    dsn := fmt.Sprintf(
        "%s:%s@%s/%s/%s?warehouse=%s&role=%s",
        "YOUR_USER",
        "YOUR_PASSWORD",
        "YOUR_ACCOUNT",
        "YOUR_DATABASE",
        "YOUR_SCHEMA",
        "YOUR_WAREHOUSE",
        "YOUR_ROLE",
    )

    db, err := gorm.Open(snowflake.Open(dsn), &gorm.Config{})
    if err != nil {
        log.Fatalf("failed to connect to Snowflake: %v", err)
    }

    fmt.Println("Successfully connected to Snowflake!") 
    _ = db
}
```