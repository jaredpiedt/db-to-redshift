# db-to-redshift
A Go library for importing data from any `sql.DB` to Redshift.

## Use
```golang
package main

import "github.com/jaredpiedt/db-to-redshift"

func main() { 
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
    err := client.Exec("SELECT syrup FROM banana.pancakes")
}
```
