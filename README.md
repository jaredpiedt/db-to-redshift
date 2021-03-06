# db-to-redshift
A Go library for importing data from any `sql.DB` to Redshift. This library performs 3 steps:

1. Retrieves data from the source database using the provided query.
2. Writes the data to a CSV file and uploads it to the provided S3 bucket.
3. Copies the S3 file into the provided Redshift database.

## Install
```bash
go get github.com/jaredpiedt/db-to-redshift
```

## Use
```golang
package main

import (
    "database/sql"
    "fmt"
    
    "github.com/aws/aws-sdk-go/aws/session"
    _ "github.com/go-sql-driver/mysql"
    "github.com/jaredpiedt/db-to-redshift"
    _ "github.com/lib/pq"
)

func main() {
    // Initialize AWS session
    session, err := session.NewSession()
    if err != nil {
        panic(err)
    }
    
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
        Session:  session,
        SourceDB: sourceDB,
        Redshift: dbtoredshift.Redshift{
            DB:               rsDB,
            Schema:           "<schema>",
            Table:            "<table>",
            CredentialsParam: "aws_iam_role=arn:aws:iam::<aws-account-id>:role/<role-name>",
            CopyParams:       "TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL TIMEFORMAT 'auto' DATEFORMAT 'auto'"
        },
        S3: dbtoredshift.S3{
            Bucket: "<bucket",
            Prefix: "<prefix>",
            Key:    "<key>",
            Region: "<region>"
        }
    }
    
    client := dbtoredshift.New(cfg)
    
    // Execute query. Data returned from that query will be inserted into Redshift
    err := client.Exec("SELECT * FROM schema.table")
}
```

## License
db-to-redshift is available under the [Apache License, Version 2.0.](http://www.apache.org/licenses/LICENSE-2.0.html)
