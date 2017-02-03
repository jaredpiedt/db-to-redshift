# db-to-redshift
A Go library for importing data from any `sql.DB` to Redshift.

## Use
```golang
package main

import (
    "database/sql"
    "fmt"
    
    _ "github.com/go-sql-driver/mysql"
    "github.com/jaredpiedt/db-to-redshift"
    _ "github.com/lib/pq"
)

func main() {
    // Open connection to source database
    sourceDB, err := sql.Open("mysql", fmt.Sprintf(
        "%s:%s@tcp(%s:3306)/%s",
        "user",
        "password",
        "host.com",
        "database",
    ))
    if err != nil {
        panic(err)
    }
    
    // Open connection to redshift database
    rsDB, err := sql.Open("pq", fmt.Sprintf(
        "user=%s password=%s dbname=%s sslmode=disable host=%s port=5439 sslmode=require",
        "user",
        "password",
        "database",
        "host.com",
    ))
    if err != nil {
        panic(err)
    }
    
    // Setup dbtoredshift config
    cfg := dbtoredshift.Config{
        SourceDB:       sourceDB,
        RedshiftDB:     rsDB,
        RedshiftTable:  "<table>",
        RedshiftSchema: "<schema>",
        S3: dbtoredshift.S3{
            Bucket:           "<bucket>",
            Region:           "<region>",
            AccessKeyID:      "<access_key_id>",
            SecretAccessKey:  "<secret_access_key>",
            Prefix:           "<prefix>",
            Key:              "<key>",
        },
        CopyParams: "",
    }
    
    client := dbtoredshift.New(cfg)
    
    // Execute query. Data returned from that query will be inserted into Redshift
    err := client.Exec("SELECT syrup FROM banana.pancakes")
}
```
