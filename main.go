package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Client wraps all database and S3 information.
type Client struct {
	cfg        Config
	awsSession *session.Session
}

// Config contains all the information needed to create a new Client.
type Config struct {
	SourceDB       *sql.DB
	RedshiftDB     *sql.DB
	RedshiftTable  string
	RedshiftSchema string
	S3             S3
	// Specify how COPY will map field data to columns in the target table,
	// define source data attributes to enable the COPY command to correctly
	// read and parse the source data, and manage which operations the COPY
	// command performs during the load process
	CopyParams string
}

// S3 contains all of the information needed to connect to an S3 bucket
type S3 struct {
	Bucket          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Prefix          string
	Key             string
}

// New will return a pointer to a new initialized db-to-redshift client.
func New(c Config) *Client {
	awsSession := session.New(&aws.Config{
		Region: aws.String(c.S3.Region),
		Credentials: credentials.NewStaticCredentials(
			c.S3.AccessKeyID,
			c.S3.SecretAccessKey,
			"",
		),
	})

	return &Client{
		cfg:        c,
		awsSession: awsSession,
	}
}

// Exec executes a query, writes the results to S3,
// and then loads them into Redshift.
// It returns any error it encounters.
func (c *Client) Exec(query string) error {
	var s3Key string

	// If prefix was passed into config, prepend it to the S3 key
	if c.cfg.S3.Prefix != "" {
		s3Key += fmt.Sprintf("%s/", c.cfg.S3.Prefix)
	}
	s3Key += c.cfg.S3.Key

	res, err := extract(c.cfg.SourceDB, query)
	if err != nil {
		return err
	}

	err = transform(res, c.awsSession, c.cfg.S3.Bucket, c.cfg.S3.Prefix, s3Key)
	if err != nil {
		return err
	}

	err = load(c.cfg.RedshiftDB, c.cfg.RedshiftSchema, c.cfg.RedshiftTable, c.cfg.S3.Bucket, s3Key, c.cfg.S3.AccessKeyID, c.cfg.S3.SecretAccessKey, c.cfg.CopyParams, c.cfg.S3.Region)

	return nil
}

// extract retrieves data from the provided database with the provided query.
// It returns a two dimensional slice of values and an error encountered.
func extract(db *sql.DB, query string) ([][]string, error) {
	var res [][]string

	rows, err := db.Query(query)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var row []string
		vals := make([]interface{}, 0, len(cols))

		for range cols {
			vals = append(vals, new(sql.NullString))
		}

		err := rows.Scan(vals...)
		if err != nil {
			return nil, err
		}

		for _, v := range vals {
			row = append(row, v.(*sql.NullString).String)
		}

		res = append(res, row)
	}

	return res, nil
}

// transform takes a two dimensional slice of values, converts them to CSV,
// and then writes the date to S3. It uses a go routine to simultaneously stream
// the CSV data to S3 so nothing has to be stored on disk.
// It returns any error that is encountered.
func transform(records [][]string, session *session.Session, bucket string, prefix string, key string) error {
	r, w := io.Pipe()

	go func() {
		csvWriter := csv.NewWriter(w)
		csvWriter.Comma = '\t'
		for _, row := range records {
			err := csvWriter.Write(row)
			if err != nil {
				// TO-DO: use channel to return error
				continue
			}
		}
		csvWriter.Flush()
		w.Close()
	}()

	uploader := s3manager.NewUploader(session)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}

	return nil
}

// load generates a Redshift COPY command and loads CSV data into Redshift.
// It returns any error it encounters.
func load(db *sql.DB, schema string, table string, bucket string, key string, accessKey string, secretKey string, copyParams string, region string) error {
	cmd := fmt.Sprintf(
		`
		COPY %s.%s
		FROM 's3://%s/%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
		%s
		REGION '%s'
		`,
		schema,
		table,
		bucket,
		key,
		accessKey,
		secretKey,
		copyParams,
		region,
	)

	_, err := db.Exec(cmd)

	return err
}
