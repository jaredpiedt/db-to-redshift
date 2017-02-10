package dbtoredshift

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/stretchr/testify/assert"
)

var Session = func() *session.Session {
	// server is the mock server that simply writes a 200 status back to the client
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	return session.Must(session.NewSession(&aws.Config{
		DisableSSL: aws.Bool(true),
		Endpoint:   aws.String(server.URL),
		Region:     aws.String("us-east-1"),
	}))
}

func TestExec(t *testing.T) {
	fmt.Println("Starting test.")
	// Setup mock AWS session
	session := Session()
	fmt.Println("Session initialized.")

	// Setup mock mysql database
	mysqlDB, mock, _ := sqlmock.New()
	defer mysqlDB.Close()

	rows := sqlmock.NewRows([]string{"col1", "col2", "col3"})
	rows.AddRow(1, 2, 3)
	rows.AddRow(4, 5, 6)
	mock.ExpectQuery("SELECT (.+) FROM test_schema.test_table").WillReturnRows(rows)

	// Setup mock redshift database
	redshiftDB, mock, _ := sqlmock.New()
	defer redshiftDB.Close()

	mock.ExpectExec(`
        COPY test_schema.test_table
        FROM 's3://test_bucket/test_prefix/test_key' CREDENTIALS ''
        
        REGION 'us-east-1'
    `).WillReturnResult(sqlmock.NewResult(2, 2))

	cfg := Config{
		Session:  session,
		SourceDB: mysqlDB,
		Redshift: Redshift{
			DB:               redshiftDB,
			Schema:           "test_schema",
			Table:            "test_table",
			CredentialsParam: "",
			CopyParams:       "",
		},
		S3: S3{
			Bucket: "test_bucket",
			Prefix: "test_prefix",
			Key:    "test_key",
		},
	}

	client := New(cfg)
	assert.NotNil(t, client)
	fmt.Println("Client created.")

	err := client.Exec("SELECT * FROM test_schema.test_table")
	assert.Nil(t, err)

	fmt.Println("Query executed.")

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
		t.FailNow()
	}
}
