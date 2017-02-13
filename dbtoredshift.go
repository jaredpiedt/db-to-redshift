package dbtoredshift

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Client wraps all database and S3 information.
type Client struct {
	cfg Config
}

// Config contains all the information needed to create a new Client.
type Config struct {
	Session      *session.Session
	SourceDB     *sql.DB
	Redshift     Redshift
	S3           S3
	CSVDelimiter rune // Default value is ','
}

// S3 contains all of the information needed to connect to an S3 bucket.
type S3 struct {
	Region string
	Bucket string
	Prefix string
	Key    string
}

// Redshift contains all of the information needed to COPY from S3 into Redshift.
type Redshift struct {
	DB     *sql.DB
	Schema string
	Table  string

	// A clause that indicates the method your cluster will use when accessing
	// your AWS S3 resource.
	CredentialsParam string

	// Specify how COPY will map field data to columns in the target table,
	// define source data attributes to enable the COPY command to correctly
	// read and parse the source data, and manage which operations the COPY
	// command performs during the load process
	CopyParams string
}

// New will return a pointer to a new db-to-redshift Client.
func New(c Config) *Client {
	// Default CSV delimiter to ','
	if c.CSVDelimiter == 0 {
		c.CSVDelimiter = ','
	}

	return &Client{
		cfg: c,
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

	res, err := c.extract(query)
	if err != nil {
		return err
	}

	err = c.transform(res, s3Key)
	if err != nil {
		return err
	}

	err = c.load(s3Key)

	return nil
}

// extract retrieves data from the provided database with the provided query.
// It returns a two dimensional slice of values and an error encountered.
func (c *Client) extract(query string) ([][]string, error) {
	var res [][]string

	rows, err := c.cfg.SourceDB.Query(query)
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
func (c *Client) transform(records [][]string, key string) error {
	r, w := io.Pipe()
	errc := make(chan error)

	go func() {
		csvWriter := csv.NewWriter(w)
		csvWriter.Comma = '\t'
		for _, row := range records {
			err := csvWriter.Write(row)
			if err != nil {
				// Add error to err channel and return
				errc <- err
				close(errc)
				return
			}
		}

		csvWriter.Flush()
		w.Close()
	}()

	go func() {
		uploader := s3manager.NewUploader(c.cfg.Session)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Body:   r,
			Bucket: aws.String(c.cfg.S3.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			errc <- err
		}

		errc <- nil
	}()

	err := <-errc
	if err != nil {
		return err
	}

	return nil
}

// load generates a Redshift COPY command and loads CSV data into Redshift.
// It returns any error it encounters.
func (c *Client) load(key string) error {
	cmd := fmt.Sprintf(
		`
		COPY %s.%s
		FROM 's3://%s/%s' CREDENTIALS '%s'
		CSV DELIMITER %s %s
		REGION '%s'
		`,
		c.cfg.Redshift.Schema,
		c.cfg.Redshift.Table,
		c.cfg.S3.Bucket,
		key,
		c.cfg.Redshift.CredentialsParam,
		strconv.QuoteRune(c.cfg.CSVDelimiter),
		c.cfg.Redshift.CopyParams,
		c.cfg.S3.Region,
	)

	_, err := c.cfg.Redshift.DB.Exec(cmd)

	return err
}
