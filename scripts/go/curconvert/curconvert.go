package curconvert

import (
	"archive/zip"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3crypto"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/SchemaHandler"

	"github.com/aws-quickstart/cur-dashboard/scripts/go/curgroups"
	"github.com/mohae/deepcopy"
)

const DecimalScale string = "10"
const DecimalPrecision string = "30"

//
type CurColumn struct {
	Name string
	Type string
}

type CurConvert struct {
	sourceBucket string
	sourceObject string
	destBucket   string
	destObject   string
	destKMSKey   string

	sourceArn        string
	sourceExternalID string
	destArn          string
	destExternalID   string

	tempDir         string
	concurrency     int
	fileConcurrency int
	compression     string

	CurColumns     []string
	CurColumnPos   map[string]int
	CurFiles       []string
	CurParqetFiles map[string]bool
	CurColumnTypes map[string]string
	skipCols       map[int]bool

	curGroups []curgroups.CurGroup

	awsLogLevel aws.LogLevelType
}

type matchAny struct {
	all []map[string]string `toml:"all"`
}

type cgMatch struct {
	any []matchAny `toml:"any"`
}

type curGroup struct {
	name        string    `toml:"name"`
	pathSuffix  string    `toml:"pathSuffix"`
	tablePrefix string    `toml:"tablePrefix"`
	match       []cgMatch `toml:"match"`
}

//
// NewCurConvert - Init struct
func NewCurConvert(sBucket string, sObject string, dBucket string, dObject string) *CurConvert {
	cur := new(CurConvert)
	cur.sourceBucket = sBucket
	cur.sourceObject = sObject
	cur.destBucket = dBucket
	cur.destObject = dObject

	cur.tempDir = "/tmp"
	cur.concurrency = 10
	cur.fileConcurrency = 30

	// over-ride CUR column types
	cur.CurColumnTypes = make(map[string]string)
	cur.CurColumnTypes["lineitem/usageamount"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/normalizationfactor"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/normalizedusageamount"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/unblendedrate"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/unblendedcost"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/blendedrate"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/blendedcost"] = "DOUBLE"
	cur.CurColumnTypes["pricing/publicondemandcost"] = "DOUBLE"
	cur.CurColumnTypes["pricing/publicondemandrate"] = "DOUBLE"
	cur.CurColumnTypes["reservation/normalizedunitsperreservation"] = "DOUBLE"
	cur.CurColumnTypes["reservation/totalreservednormalizedunits"] = "DOUBLE"
	cur.CurColumnTypes["reservation/totalreservedunits"] = "DOUBLE"
	cur.CurColumnTypes["reservation/unitsperreservation"] = "DOUBLE"

	// init parquet file map
	cur.CurParqetFiles = make(map[string]bool)

	// set Log Level through ENV
	cur.awsLogLevel = cur.setLogLevel()

	return cur
}

//
//
func (c *CurConvert) setLogLevel() aws.LogLevelType {
	switch logLevel := os.Getenv("AWS_LOG_LEVEL"); logLevel {
	case "DEBUG":
		return aws.LogDebug
	case "DEBUG_SIGNING":
		return aws.LogDebugWithSigning
	case "DEBUG_BODY":
		return aws.LogDebugWithSigning | aws.LogDebugWithHTTPBody
	default:
		return aws.LogOff
	}
}

//
//
func (c *CurConvert) SetCurGroups(curGroups ...curgroups.CurGroup) {
	for _, group := range curGroups {
		c.curGroups = append(c.curGroups, group)
	}
}

//
// SetFileConcurrency - Allows for over-ride of number of CUR files processed concurrently
func (c *CurConvert) SetFileConcurrency(concurrency int) error {
	if concurrency < 1 || concurrency > 1000 {
		return errors.New("File Concurrency must be between 1-1000")
	}
	c.fileConcurrency = concurrency
	return nil
}

//
// SetSourceRole - configures the source profile data for retrieving CUR from different AWS account
func (c *CurConvert) SetSourceRole(arn string, externalID string) error {
	if len(arn) < 1 {
		return errors.New("Must supply a Role ARN")
	}
	c.sourceArn = arn
	c.sourceExternalID = externalID
	return nil
}

//
// SetDestRole - configures the source profile data for retrieving CUR from different AWS account
func (c *CurConvert) SetDestRole(arn string, externalID string) error {
	if len(arn) < 1 {
		return errors.New("Must supply a Role ARN")
	}
	c.destArn = arn
	c.destExternalID = externalID
	return nil
}

//
// SetCURManifest - configures the source manifest object for retrieving CUR from different AWS account
func (c *CurConvert) SetSourceManifest(manifest string) error {
	if len(manifest) < 1 {
		return errors.New("Must supply a Manifest")
	}
	c.sourceObject = manifest
	return nil
}

//
// SetDestPath - configures the dest pat for the converted CUR
func (c *CurConvert) SetDestPath(path string) error {
	if len(path) < 1 {
		return errors.New("Must supply a Path")
	}
	c.destObject = path
	return nil
}

//
// SetDestKMSKey - sets the KMS Master key arn to use for
func (c *CurConvert) SetDestKMSKey(key string) error {
	if len(key) < 1 {
		return errors.New("Must supply a Key ARN")
	}
	c.destKMSKey = key
	return nil
}

//
// SetTmpLocation - sets the temp directory for CUR files to be downloaded to, and parquet files to be written too
func (c *CurConvert) SetTmpLocation(path string) error {
	r, err := regexp.Compile("^/[A-Z,a-z,0-9,_,-,/]+[^/]$")
	if err != nil {
		return fmt.Errorf("Failed to set TempLocation, regexp error: %s", err)
	}
	if len(path) < 1 || !r.MatchString(path) {
		return errors.New("Must supply a valid path that starts with '/' and is not terminated with '/'")
	}
	c.tempDir = path
	return nil
}

//
// GetCURColumns - Converts processed CUR columns into map and returns it
func (c *CurConvert) GetCURColumns() ([]CurColumn, error) {

	if len(c.CurColumns) < 1 {
		return nil, errors.New("Cannot fetch CUR column data, call ParseCUR first")
	}

	sh := SchemaHandler.NewSchemaHandlerFromMetadata(c.CurColumns)
	cols := []CurColumn{}

	for i := range sh.SchemaElements {
		if sh.SchemaElements[i].Type == nil {
			continue
		}

		var t string
		if sh.SchemaElements[i].ConvertedType != nil {
			t = sh.SchemaElements[i].ConvertedType.String()
		} else if sh.SchemaElements[i].Type != nil {
			t = sh.SchemaElements[i].Type.String()
		} else {
			return nil, errors.New("Cannot fetch CUR column data, Failed to find Type for CurColumn")
		}

		if t == "UTF8" {
			t = "STRING"
		}
		cols = append(cols, CurColumn{Name: sh.SchemaElements[i].GetName(), Type: t})
	}
	return cols, nil
}

func (c *CurConvert) getCreds(arn string, externalID string, sess *session.Session) *credentials.Credentials {
	if len(arn) < 1 {
		return nil
	}
	if len(externalID) > 0 {
		return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {
			p.ExternalID = &externalID
		})
	}
	return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {})
}

func (c *CurConvert) getBucketLocation(bucket string, arn string, externalID string) (string, error) {

	// Init Session
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-east-1"),
		LogLevel: aws.LogLevel(c.awsLogLevel),
	})
	if err != nil {
		return "", err
	}

	// if needed set creds for AssumeRole and reset session
	if len(arn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(arn, externalID, sess)})
	}

	// Get Bucket location
	svc := s3.New(sess)
	res, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		return "", err
	}

	// empty string returned for buckets existing in us-east-1! https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETlocation.html
	if res.LocationConstraint == nil || len(*res.LocationConstraint) < 1 {
		return "us-east-1", nil
	}
	return *res.LocationConstraint, nil
}

func (c *CurConvert) initS3Downloader(bucket string, arn string, externalID string) (*s3manager.Downloader, error) {

	// get location of bucket
	bucketLocation, err := c.getBucketLocation(bucket, arn, externalID)
	if err != nil {
		return nil, err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{
		DisableRestProtocolURICleaning: aws.Bool(true),
		Region:   aws.String(bucketLocation),
		LogLevel: aws.LogLevel(c.awsLogLevel),
	})
	if err != nil {
		return nil, err
	}

	// if needed set creds for AssumeRole and reset session
	if len(arn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(arn, externalID, sess)})
	}

	return s3manager.NewDownloader(sess), nil
}

func (c *CurConvert) initS3Uploader(bucket string, arn string, externalID string) (*s3manager.Uploader, error) {

	// get location of bucket
	bucketLocation, err := c.getBucketLocation(bucket, arn, externalID)
	if err != nil {
		return nil, err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{
		DisableRestProtocolURICleaning: aws.Bool(true),
		Region:   aws.String(bucketLocation),
		LogLevel: aws.LogLevel(c.awsLogLevel),
	})
	if err != nil {
		return nil, err
	}

	// if needed set creds for AssumeRole and reset session
	if len(arn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(arn, externalID, sess)})
	}

	return s3manager.NewUploader(sess), nil
}

//
// CheckCURExists - Attempts to fetch manifest file to confirm existence of CUR
func (c *CurConvert) CheckCURExists() error {

	// get location of bucket
	bucketLocation, err := c.getBucketLocation(c.sourceBucket, c.sourceArn, c.sourceExternalID)
	if err != nil {
		return err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{
		DisableRestProtocolURICleaning: aws.Bool(true),
		Region:   aws.String(bucketLocation),
		LogLevel: aws.LogLevel(c.awsLogLevel),
	})
	if err != nil {
		return err
	}

	// if needed set creds for AssumeRole and reset session
	if len(c.sourceArn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(c.sourceArn, c.sourceExternalID, sess)})
	}

	svc := s3.New(sess)
	_, err = svc.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(c.sourceBucket),
			Key:    aws.String(c.sourceObject),
		})

	return err
}

//
// ParseCur - Reads JSON manifest file from S3 and adds needed data into struct
func (c *CurConvert) ParseCur() error {

	// init S3 manager
	s3dl, err := c.initS3Downloader(c.sourceBucket, c.sourceArn, c.sourceExternalID)
	if err != nil {
		return err
	}

	// Download CUR manifest JSON
	buff := &aws.WriteAtBuffer{}
	_, err = s3dl.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(c.sourceBucket),
		Key:    aws.String(c.sourceObject),
	})
	if err != nil {
		return fmt.Errorf("failed to download manifest, bucket: %s, object: %s, error: %s", c.sourceBucket, c.sourceObject, err.Error())
	}

	// Unmarshall JSON
	var j map[string]interface{}
	err = json.Unmarshal(buff.Bytes(), &j)
	if err != nil {
		return fmt.Errorf("failed to parse manifest, bucket: %s, object: %s, error: %s", c.sourceBucket, c.sourceObject, err.Error())
	}

	// store compression type
	c.compression = j["compression"].(string)

	// Store all column names from manifests
	cols := j["columns"].([]interface{})
	seen := make(map[string]bool)
	c.skipCols = make(map[int]bool)
	c.CurColumnPos = make(map[string]int)
	i := -1
	for column := range cols {
		i++
		t := cols[column].(map[string]interface{})
		columnName := t["category"].(string) + "/" + t["name"].(string)

		// convert columns names to allowed characters (lowercase) and substitute '_' for any non-allowed character
		columnName = strings.ToLower(columnName)
		r := func(r rune) rune {
			switch {
			case r >= 'a' && r <= 'z':
				return r
			case r >= '0' && r <= '9':
				return r
			case r == '/':
				return r
			default:
				return '_'
			}
		}
		columnName = strings.Map(r, columnName)

		// Skip duplicate columns
		if _, ok := seen[columnName]; ok {
			c.skipCols[i] = true
			continue
		}
		// Check for type over-ride
		colType, ok := c.CurColumnTypes[columnName]
		if !ok {
			colType = "UTF8"
		}

		if colType == "DECIMAL" {
			colType += ", scale=" + DecimalScale + ", precision=" + DecimalPrecision + ", basetype=FIXED_LEN_BYTE_ARRAY"
		}

		encoding := ""
		if colType == "UTF8" {
			encoding = ", encoding=PLAIN_DICTIONARY"
		}
		c.CurColumns = append(c.CurColumns, "name="+columnName+", type="+colType+encoding)
		c.CurColumnPos[columnName] = i
		seen[columnName] = true
	}

	// Store CSV CUR files
	reportKeys := j["reportKeys"].([]interface{})
	for key := range reportKeys {
		c.CurFiles = append(c.CurFiles, reportKeys[key].(string))
	}
	return nil
}

//
// DownloadCur -
func (c *CurConvert) DownloadCur(curObject string) (string, error) {

	// define localfile name
	localFile := c.tempDir + "/" + curObject[strings.LastIndex(curObject, "/")+1:]

	// create localfile
	file, err := os.Create(localFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// init S3 manager
	s3dl, err := c.initS3Downloader(c.sourceBucket, c.sourceArn, c.sourceExternalID)
	if err != nil {
		return "", err
	}

	// download S3 object to file
	_, err = s3dl.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(c.sourceBucket),
			Key:    aws.String(curObject),
		})

	if err != nil {
		return "", fmt.Errorf("failed to download CUR object, bucket: %s, object: %s, error: %s", c.sourceBucket, curObject, err.Error())
	}

	return localFile, nil
}

//
// ParquetCur -
func (c *CurConvert) ParquetCur(inputFile string) (string, []curgroups.CurGroup, error) {

	// deep copy CurGroups
	groups := deepcopy.Copy(c.curGroups).([]curgroups.CurGroup)

	// open input
	file, err := os.Open(inputFile)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()

	var gr io.Reader
	// init compression library on input
	switch c.compression {
	case "GZIP":
		gr, err = gzip.NewReader(file)
	case "ZIP":
		fi, err := file.Stat()
		if err != nil {
			return "", nil, err
		}
		size := fi.Size()
		r, err := zip.NewReader(file, size)
		if err != nil {
			return "", nil, err
		}
		var files []io.Reader
		for _, file := range r.File {
			f, err := file.Open()
			if err != nil {
				return "", nil, err
			}
			files = append(files, f)
		}
		gr = io.MultiReader(files...)
		err = nil
	}
	if err != nil {
		log.Fatal(err)
	}
	// defer gr.Close()

	// init csv reader
	cr := csv.NewReader(gr)

	// read and ignore header record
	_, err = cr.Read()
	if err != nil {
		log.Fatal(err)
	}

	// create local parquet file
	outputFile := inputFile[strings.LastIndex(inputFile, "/")+1:strings.Index(inputFile, ".")] + ".parquet"
	localParquetFile := c.tempDir + "/" + outputFile
	f, err := ParquetFile.NewLocalFileWriter(localParquetFile)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create parquet file %s, error: %s", localParquetFile, err.Error())
	}

	// create any required curGroup parquet files
	for j := range c.curGroups {
		localGroupParquetFile := c.tempDir + "/" + groups[j].Name + "_" + outputFile
		if err := groups[j].CreateParquetWriter(localGroupParquetFile, c.CurColumns, c.CurColumnPos, c.concurrency); err != nil {
			return "", nil, fmt.Errorf("failed to create curGroup writer %s, error: %s", localGroupParquetFile, err.Error())
		}
	}

	// init Parquet writer
	ph, err := ParquetWriter.NewCSVWriter(c.CurColumns, f, int64(c.concurrency))
	if err != nil {
		return "", nil, err
	}

	// read all remaining records of CSV file and write to parquet
	i := 1
	for {
		if i%5000 == 0 {
			ph.Flush(true)
			i = 1
		}

		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", nil, err
		}

		var recParquet []*string
		for k := range rec {
			_, skip := c.skipCols[k]
			if !skip {
				recParquet = append(recParquet, &rec[k])
			}
		}
		ph.WriteString(recParquet)
		for j := range groups {
			groups[j].Writer <- recParquet
		}
		i++
	}

	if i > 1 {
		ph.Flush(true)
	}
	ph.WriteStop()
	f.Close()

	// close any required curGroup parquet files and add to upload map
	for j := range groups {
		groups[j].CloseParquetWriter()
	}

	return localParquetFile, groups, nil
}

func (c *CurConvert) uploadCUR(destObject string, file io.Reader) error {

	// init S3 manager
	s3up, err := c.initS3Uploader(c.destBucket, c.destArn, c.destExternalID)
	if err != nil {
		return err
	}

	_, err = s3up.Upload(&s3manager.UploadInput{
		Bucket: aws.String(c.destBucket),
		Key:    aws.String(destObject),
		Body:   file,
	})

	if err != nil {
		return fmt.Errorf("failed to upload CUR parquet object, bucket: %s, object: %s, error: %s", c.destBucket, destObject, err.Error())
	}

	return nil
}

func (c *CurConvert) uploadEncryptedCUR(destObject string, file io.ReadSeeker) error {

	// get location of bucket
	bucketLocation, err := c.getBucketLocation(c.destBucket, c.destArn, c.destExternalID)
	if err != nil {
		return err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{
		DisableRestProtocolURICleaning: aws.Bool(true),
		Region:   aws.String(bucketLocation),
		LogLevel: aws.LogLevel(c.awsLogLevel),
	})
	if err != nil {
		return err
	}

	// if needed set creds for AssumeRole and reset session
	if len(c.destArn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(c.destArn, c.destExternalID, sess)})
	}

	// init crypto lib
	handler := s3crypto.NewKMSKeyGenerator(kms.New(sess), c.destKMSKey)
	encryptionClient := s3crypto.NewEncryptionClient(sess, s3crypto.AESGCMContentCipherBuilder(handler))

	req, _ := encryptionClient.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(c.destBucket),
		Key:    aws.String(destObject),
		Body:   file,
	})

	err = req.Send()
	if err != nil {
		return fmt.Errorf("failed to upload CUR parquet object, bucket: %s, object: %s, error: %s", c.destBucket, destObject, err.Error())
	}

	return nil
}

//
// UploadCur -
func (c *CurConvert) UploadCur(parquetFile string, groups []curgroups.CurGroup) error {

	files := make(map[string]string)
	for i := range groups {
		localFile, destPathPrefix := groups[i].GetUploadData()
		files[localFile] = destPathPrefix
	}
	files[parquetFile] = "main"

	for file, prefix := range files {
		destObject := c.destObject + "/" + prefix + "/" + file[strings.LastIndex(file, "/")+1:]

		f, err := os.Open(file)
		if err != nil {
			return err
		}

		if len(c.destKMSKey) > 0 {
			if err := c.uploadEncryptedCUR(destObject, f); err != nil {
				return err
			}
		} else {
			if err := c.uploadCUR(destObject, f); err != nil {
				return err
			}
		}
		f.Close()
		c.CurParqetFiles[destObject] = true
	}

	return nil
}

//
// CleanCUr
func (c *CurConvert) CleanCur() error {

	// init S3 manager
	s3up, err := c.initS3Uploader(c.destBucket, c.destArn, c.destExternalID)
	if err != nil {
		return err
	}

	// List all objects in current parquet destination path
	result, err := s3up.S3.ListObjectsV2(
		&s3.ListObjectsV2Input{
			Bucket:  aws.String(c.destBucket),
			Prefix:  aws.String(c.destObject + "/"),
			MaxKeys: aws.Int64(500),
		})
	if err != nil {
		return fmt.Errorf("Error listing oject list when cleaning CUR: %s", err.Error())
	}

	// Build delete list of all objects not in c.CurParqetFiles map i.e. have not been uploaded on this conversion.
	var deleteObjects []s3manager.BatchDeleteObject
	for object := range result.Contents {
		_, ok := c.CurParqetFiles[*result.Contents[object].Key]
		if !ok {
			deleteObjects = append(deleteObjects, s3manager.BatchDeleteObject{
				Object: &s3.DeleteObjectInput{
					Key:    aws.String(*result.Contents[object].Key),
					Bucket: aws.String(c.destBucket),
				},
			})
		}
	}

	// Proccess object delection / cleanup
	batcher := s3manager.NewBatchDeleteWithClient(s3up.S3)
	err = batcher.Delete(aws.BackgroundContext(), &s3manager.DeleteObjectsIterator{
		Objects: deleteObjects,
	})
	if err != nil {
		return fmt.Errorf("Error deleting objects when cleaning CUR: %s", err.Error())
	}
	return nil
}

//
// ConvertCur - Performs Download, Conversion
func (c *CurConvert) ConvertCur() error {
	if err := c.ParseCur(); err != nil {
		return fmt.Errorf("Error Parsing CUR Manifest: %s", err.Error())
	}

	result := make(chan error)
	limit := make(chan bool, c.fileConcurrency)
	i := 0
	for reportKey := range c.CurFiles {
		go func(object string) {
			limit <- true
			gzipFile, err := c.DownloadCur(object)
			if err != nil {
				result <- fmt.Errorf("Error Downloading CUR: %s", err.Error())
				return
			}

			parquetFile, groups, err := c.ParquetCur(gzipFile)
			if err != nil {
				result <- fmt.Errorf("Error Converting CUR: %s", err.Error())
				return
			}

			if err := c.UploadCur(parquetFile, groups); err != nil {
				result <- fmt.Errorf("Error Uploading CUR: %s", err.Error())
				return
			}

			os.Remove(gzipFile)
			os.Remove(parquetFile)
			for i := range c.curGroups {
				file, _ := c.curGroups[i].GetUploadData()
				os.Remove(file)
			}
			<-limit
			result <- nil
		}(c.CurFiles[reportKey])
		i++
	}

	// wait for jobs to complete
	for w := 0; w < i; w++ {
		err := <-result
		if err != nil {
			return err
		}
	}

	return c.CleanCur()
}
