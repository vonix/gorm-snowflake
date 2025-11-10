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

    snowflake "github.com/vonix/gorm-snowflake"
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

### Key-Pair Authentication

For enhanced security, you can use RSA key-pair authentication instead of passwords:

```go
package main

import (
    "fmt"
    "log"

    snowflake "github.com/vonix/gorm-snowflake"
    "gorm.io/gorm"
)

func main() {
    privateKeyPEM := `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC...
-----END PRIVATE KEY-----`

    dialector := snowflake.OpenWithKey(
        "YOUR_ACCOUNT",     
        "YOUR_USER",        
        privateKeyPEM,      
        "YOUR_DATABASE",    
        "YOUR_SCHEMA",      
        "YOUR_WAREHOUSE",   
        "YOUR_ROLE",        
    )

    db, err := gorm.Open(dialector, &gorm.Config{})
    if err != nil {
        log.Fatalf("failed to connect to Snowflake with key-pair auth: %v", err)
    }

    fmt.Println("Successfully connected to Snowflake with key-pair authentication!")
    _ = db
}
```

#### Setting Up Key-Pair Authentication

1. **Generate RSA Key Pair:**
   ```bash
   # Generate private key
   openssl genrsa -out snowflake_key.pem 2048
   
   # Generate public key
   openssl rsa -in snowflake_key.pem -pubout -out snowflake_key.pub
   ```

2. **Register Public Key with Snowflake:**
   ```sql
   ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';
   ```

3. **Load Private Key in Your Application:**
   ```go
   privateKeyBytes, err := os.ReadFile("snowflake_key.pem")
   if err != nil {
       log.Fatal(err)
   }
   privateKeyPEM := string(privateKeyBytes)
   
   privateKeyPEM := os.Getenv("SNOWFLAKE_PRIVATE_KEY")
   ```

**Security Notes:**
- Store private keys securely (environment variables, secret management systems)
- Never commit private keys to version control

## Authentication Methods

| Method | Security | Setup Complexity |
|--------|----------|------------------|
| **Password (DSN)** | Basic | Low |
| **Key-Pair** | High | Medium |
```