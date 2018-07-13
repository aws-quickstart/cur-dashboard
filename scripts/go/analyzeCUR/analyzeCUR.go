package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/aws-quickstart/cur-dashboard/scripts/go/curconvert"
	"github.com/aws-quickstart/cur-dashboard/scripts/go/curgroups"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/jcxplorer/cwlogger"
)

type General struct {
	Namespace string
}

type RI struct {
	Enabled          bool `toml:"enableRIanalysis"`
	TotalUtilization bool `toml:"enableRITotalUtilization"`
	PercentThreshold int  `toml:"riPercentageThreshold"`
	TotalThreshold   int  `toml:"riTotalThreshold"`
	CwName           string
	CwNameTotal      string
	CwDimension      string
	CwDimensionTotal string
	CwType           string
	Sql              string
	Ignore           map[string]int
}

type Metric struct {
	Enabled     bool
	Hourly      bool
	Daily       bool
	Monthly     bool
	Type        string
	SQL         string
	CwName      string
	CwDimension string
	CwType      string
}

type Athena struct {
	DbSQL       string `toml:"create_database"`
	TablePrefix string `toml:"table_prefix"`
	TableSQL    string `toml:"create_table"`
	DbName      string `toml:"database_name"`
}

type MetricConfig struct {
	Substring map[string]string
}

type Config struct {
	General      General
	RI           RI
	Athena       Athena
	MetricConfig MetricConfig
	Metrics      []Metric
	CurGroups    []curgroups.CurGroup
}

type AthenaResponse struct {
	Rows []map[string]string
}

var defaultConfigPath = "./scripts/analyzeCUR.config"
var maxConcurrentQueries = 5

func getInstanceMetadata(sess *session.Session) map[string]interface{} {
	c := &http.Client{
		Timeout: 100 * time.Millisecond,
	}
	resp, err := c.Get("http://169.254.169.254/latest/dynamic/instance-identity/document")
	var m map[string]interface{}
	if err == nil {
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			err = json.Unmarshal(body, &m)
			if err != nil {
				log.Fatalln("Could not parse MetaData, erorr: " + err.Error())
			}
		}
	}
	// if we havent obtained instance meta-data fetch account from STS - likely were not on EC2
	_, ok := m["region"].(string)
	if !ok {
		svc := sts.New(sess)
		result, err := svc.GetCallerIdentity(&sts.GetCallerIdentityInput{})
		if err == nil {
			m = make(map[string]interface{})
			m["accountId"] = *result.Account
			m["region"] = *sess.Config.Region
		}
	}
	return m
}

/*
Function reads in and validates command line parameters
*/
func getParams(configFile *string, sourceBucket *string, destBucket *string, account *string, curReportName *string, curReportPath *string, curDestPath *string, dateOverride *string) error {

	// Define input command line config parameter and parse it
	flag.StringVar(configFile, "config", defaultConfigPath, "Input config file for analyzeDBR")
	flag.StringVar(sourceBucket, "bucket", "", "AWS Bucket where CUR files sit")
	flag.StringVar(destBucket, "destbucket", "", "AWS Bucket where where Parquet files will be uploaded (Optional - use as override-only) ")
	flag.StringVar(account, "account", "", "AWS Account #")
	flag.StringVar(curReportName, "reportname", "", "CUR Report Name")
	flag.StringVar(curReportPath, "reportpath", "", "CUR Report PAth")
	flag.StringVar(curDestPath, "destpath", "", "Destination Path for converted CUR to be uploaded too")
	flag.StringVar(dateOverride, "date", "", "Optional date flag to over-ride the processing CUR month")

	flag.Parse()

	// check input against defined regex's
	regexEmpty := regexp.MustCompile(`^$`)
	regexAccount := regexp.MustCompile(`^\d+$`)

	if regexEmpty.MatchString(*sourceBucket) {
		return errors.New("Config Error: Must provide valid AWS DBR bucket")
	}

	if !regexAccount.MatchString(*account) {
		return errors.New("Config Error: Must provide valid AWS account number")
	}
	if regexEmpty.MatchString(*curReportName) {
		return errors.New("Config Error: Must provide valid CUR Report Name")
	}
	if destBucket == nil || len(*destBucket) < 1 {
		*destBucket = *sourceBucket
	}

	return nil
}

/*
Function reads in configuration file provided in configFile input
Config file is stored in TOML format
*/
func getConfig(conf *Config, configFile string) error {

	// check for existance of file
	if _, err := os.Stat(configFile); err != nil {
		return errors.New("Config File " + configFile + " does not exist")
	}

	// read file
	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		return errors.New("Error Reading TOML config file: " + err.Error())
	}

	// parse TOML config file into struct
	if _, err := toml.Decode(string(b), &conf); err != nil {
		return errors.New("Error Decoding TOML config file: " + err.Error())
	}

	return nil
}

/*
Function substitutes parameters into SQL command.
Input map contains key (thing to look for in input sql) and if found replaces with given value)
*/
func substituteParams(sql string, params map[string]string) string {

	for sub, value := range params {
		sql = strings.Replace(sql, sub, value, -1)
	}

	return sql
}

/*
Function takes SQL to send to Athena converts into JSON to send to Athena HTTP proxy and then sends it.
Then recieves responses in JSON which is converted back into a struct and returned
*/
func sendQuery(svc *athena.Athena, db string, sql string, account string, region string) (AthenaResponse, error) {

	var results AthenaResponse
	var s athena.StartQueryExecutionInput
	s.SetQueryString(sql)

	var q athena.QueryExecutionContext
	q.SetDatabase(db)
	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	r.SetOutputLocation("s3://aws-athena-query-results-" + account + "-" + region + "/")
	s.SetResultConfiguration(&r)

	result, err := svc.StartQueryExecution(&s)
	if err != nil {
		return results, errors.New("Error Querying Athena, StartQueryExecution: " + err.Error())
	}

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*result.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(2) * time.Second // Pause for 2 seconds

	for {
		qrop, err = svc.GetQueryExecution(&qri)
		if err != nil {
			return results, errors.New("Error Querying Athena, GetQueryExecution: " + err.Error())
		}
		if *qrop.QueryExecution.Status.State != "RUNNING" {
			break
		}
		time.Sleep(duration)
	}

	if *qrop.QueryExecution.Status.State != "SUCCEEDED" {
		return results, errors.New("Error Querying Athena, completion state is NOT SUCCEEDED, QueryExecution is: " + qrop.QueryExecution.GoString())
	}

	var ip athena.GetQueryResultsInput
	ip.SetQueryExecutionId(*result.QueryExecutionId)

	// loop through results (paginated call)
	var colNames []string
	err = svc.GetQueryResultsPages(&ip,
		func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
			for row := range page.ResultSet.Rows {
				if len(colNames) < 1 { // first row contains column names - which we use in any subsequent rows to produce map[columnname]values
					for j := range page.ResultSet.Rows[row].Data {
						colNames = append(colNames, *page.ResultSet.Rows[row].Data[j].VarCharValue)
					}
				} else {
					result := make(map[string]string)
					skip := false
					for j := range page.ResultSet.Rows[row].Data {
						if j < len(colNames) {
							if page.ResultSet.Rows[row].Data[j].VarCharValue == nil {
								skip = true
								break
							}
							result[colNames[j]] = *page.ResultSet.Rows[row].Data[j].VarCharValue
						}
					}
					if len(result) > 0 && !skip {
						results.Rows = append(results.Rows, result)
					}
				}
			}
			if lastPage {
				return false // return false to end paginated calls
			}
			return true // keep going if there are more pages to fetch
		})
	if err != nil {
		return results, errors.New("Error Querying Athena, GetQueryResultsPages: " + err.Error())
	}

	return results, nil
}

/*
Function takes metric data (from Athena etal) and sends through to cloudwatch.
*/
func sendMetric(svc *cloudwatch.CloudWatch, data AthenaResponse, cwNameSpace string, cwName string, cwType string, cwDimensionName string, interval string) error {

	input := cloudwatch.PutMetricDataInput{}
	input.Namespace = aws.String(cwNameSpace)
	i := 0
	for row := range data.Rows {
		// skip metric if dimension or value is empty
		if len(data.Rows[row]["dimension"]) < 1 || len(data.Rows[row]["value"]) < 1 {
			continue
		}

		// send Metric Data as we have reached 20 records, and clear MetricData Array
		if i >= 20 {
			_, err := svc.PutMetricData(&input)
			if err != nil {
				return errors.New("Could sending CW Metric: " + err.Error())
			}
			input.MetricData = nil
			i = 0
		}
		var t time.Time
		if interval == "hourly" {
			t, _ = time.Parse("2006-01-02T15", data.Rows[row]["date"])
		} else if interval == "daily" {
			t, _ = time.Parse("2006-01-02", data.Rows[row]["date"])
		} else {
			t, _ = time.Parse("2006-01", data.Rows[row]["date"])
		}

		v, _ := strconv.ParseFloat(data.Rows[row]["value"], 64)
		metric := cloudwatch.MetricDatum{
			MetricName: aws.String(cwName),
			Timestamp:  aws.Time(t),
			Unit:       aws.String(cwType),
			Value:      aws.Float64(v),
		}

		// Dimension can be a single or comma seperated list of values, or key/values
		// presence of "=" sign in value designates key=value. Otherwise input cwDimensionName is used as key
		d := strings.Split(data.Rows[row]["dimension"], ",")
		for i := range d {
			var dname, dvalue string
			if strings.Contains(d[i], "=") {
				dTuple := strings.Split(d[i], "=")
				dname = dTuple[0]
				dvalue = dTuple[1]
			} else {
				dname = cwDimensionName
				dvalue = d[i]
			}
			cwD := cloudwatch.Dimension{
				Name:  aws.String(dname),
				Value: aws.String(dvalue),
			}
			metric.Dimensions = append(metric.Dimensions, &cwD)
		}

		// add interval metric
		metric.Dimensions = append(metric.Dimensions, &cloudwatch.Dimension{Name: aws.String("interval"), Value: aws.String(interval)})

		// append to overall Metric Data
		input.MetricData = append(input.MetricData, &metric)
		i++
	}

	// if we still have data to send - send it
	if len(input.MetricData) > 0 {
		_, err := svc.PutMetricData(&input)
		if err != nil {
			return errors.New("Could sending CW Metric: " + err.Error())
		}
	}

	return nil
}

/*
Function processes a single hours worth of RI usage and compares against available RIs to produce % utiization / under-utilization
*/
func riUtilizationHour(svc *cloudwatch.CloudWatch, date string, used map[string]map[string]map[string]int, azRI map[string]map[string]map[string]int, regionRI map[string]map[string]int, conf Config, region string) error {

	// // Perform Deep Copy of both RI maps.
	// // We need a copy of the maps as we decrement the RI's available by the hourly usage and a map is a pointer
	// // hence decrementing the original maps will affect the pass-by-reference data
	// cpy := deepcopy.Copy(azRI)
	// t_azRI, ok := cpy.(map[string]map[string]map[string]int)
	// if !ok {
	// 	return errors.New("could not copy AZ RI map")
	// }

	// cpy = deepcopy.Copy(regionRI)
	// t_regionRI, ok := cpy.(map[string]map[string]int)
	// if !ok {
	// 	return errors.New("could not copy Regional RI map")
	// }

	// // Iterate through used hours decrementing any available RI's per hour's that were used
	// // AZ specific RI's are first checked and then regional RI's
	// for az := range used {
	// 	for instance := range used[az] {
	// 		// check if azRI for this region even exist
	// 		_, ok := t_azRI[az][instance]
	// 		if ok {
	// 			for platform := range used[az][instance] {
	// 				// check if azRI for this region and platform even exists
	// 				_, ok2 := t_azRI[az][instance][platform]
	// 				if ok2 {
	// 					// More RI's than we used
	// 					if t_azRI[az][instance][platform] >= used[az][instance][platform] {
	// 						t_azRI[az][instance][platform] -= used[az][instance][platform]
	// 						used[az][instance][platform] = 0
	// 					} else {
	// 						// Less RI's than we used
	// 						used[az][instance][platform] -= t_azRI[az][instance][platform]
	// 						t_azRI[az][instance][platform] = 0
	// 					}
	// 				}
	// 			}
	// 		}

	// 		// check if regionRI even exists and that instance used is in the right region
	// 		_, ok = t_regionRI[instance]
	// 		if ok && az[:len(az)-1] == region {
	// 			for platform := range used[az][instance] {
	// 				// if we still have more used instances check against regional RI's
	// 				if used[az][instance][platform] > 0 && t_regionRI[instance][platform] > 0 {
	// 					if t_regionRI[instance][platform] >= used[az][instance][platform] {
	// 						t_regionRI[instance][platform] -= used[az][instance][platform]
	// 						used[az][instance][platform] = 0
	// 					} else {
	// 						used[az][instance][platform] -= t_regionRI[instance][platform]
	// 						t_regionRI[instance][platform] = 0
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	// // Now loop through the temp RI data to check if any RI's are still available
	// // If they are and the % of un-use is above the configured threshold then colate for sending to cloudwatch
	// // We sum up the total of regional and AZ specific RI's so that we get one instance based metric regardless of region or AZ RI
	// i_unused := make(map[string]map[string]int)
	// i_total := make(map[string]map[string]int)
	// var unused int
	// var total int

	// for az := range t_azRI {
	// 	for instance := range t_azRI[az] {
	// 		_, ok := i_unused[instance]
	// 		if !ok {
	// 			i_unused[instance] = make(map[string]int)
	// 			i_total[instance] = make(map[string]int)
	// 		}
	// 		for platform := range t_azRI[az][instance] {
	// 			i_total[instance][platform] = azRI[az][instance][platform]
	// 			i_unused[instance][platform] = t_azRI[az][instance][platform]
	// 			total += azRI[az][instance][platform]
	// 			unused += t_azRI[az][instance][platform]
	// 		}
	// 	}
	// }

	// for instance := range t_regionRI {
	// 	for platform := range t_regionRI[instance] {
	// 		_, ok := i_unused[instance]
	// 		if !ok {
	// 			i_unused[instance] = make(map[string]int)
	// 			i_total[instance] = make(map[string]int)
	// 		}
	// 		i_total[instance][platform] += regionRI[instance][platform]
	// 		i_unused[instance][platform] += t_regionRI[instance][platform]
	// 		total += regionRI[instance][platform]
	// 		unused += t_regionRI[instance][platform]
	// 	}
	// }

	// // loop over per-instance utilization and build metrics to send
	// metrics := AthenaResponse{}
	// for instance := range i_unused {
	// 	_, ok := conf.RI.Ignore[instance]
	// 	if !ok { // instance not on ignore list
	// 		for platform := range i_unused[instance] {
	// 			percent := (float64(i_unused[instance][platform]) / float64(i_total[instance][platform])) * 100
	// 			if int(percent) > conf.RI.PercentThreshold && i_total[instance][platform] > conf.RI.TotalThreshold {
	// 				metrics.Rows = append(metrics.Rows, map[string]string{"dimension": "instance=" + instance + ",platform=" + platform, "date": date, "value": strconv.FormatInt(int64(percent), 10)})
	// 			}
	// 		}
	// 	}
	// }

	// // send per instance type under-utilization
	// if len(metrics.Rows) > 0 {
	// 	if err := sendMetric(svc, metrics, conf.General.Namespace, conf.RI.CwName, conf.RI.CwType, conf.RI.CwDimension); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	// // If confured send overall total utilization
	// if conf.RI.TotalUtilization {
	// 	percent := 100 - ((float64(unused) / float64(total)) * 100)
	// 	total := AthenaResponse{}
	// 	total.Rows = append(total.Rows, map[string]string{"dimension": "hourly", "date": date, "value": strconv.FormatInt(int64(percent), 10)})
	// 	if err := sendMetric(svc, total, conf.General.Namespace, conf.RI.CwNameTotal, conf.RI.CwType, conf.RI.CwDimensionTotal); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	return nil
}

/*
Main RI function. Gest RI and usage data (from Athena).
Then loops through every hour and calls riUtilizationHour to process each hours worth of data
*/
func riUtilization(sess *session.Session, svcAthena *athena.Athena, conf Config, key string, secret string, region string, account string, date string) error {

	// svc := ec2.New(sess)

	// params := &ec2.DescribeReservedInstancesInput{
	// 	DryRun: aws.Bool(false),
	// 	Filters: []*ec2.Filter{
	// 		{
	// 			Name: aws.String("state"),
	// 			Values: []*string{
	// 				aws.String("active"),
	// 			},
	// 		},
	// 	},
	// }

	// resp, err := svc.DescribeReservedInstances(params)
	// if err != nil {
	// 	return err
	// }

	// az_ri := make(map[string]map[string]map[string]int)
	// region_ri := make(map[string]map[string]int)

	// // map in number of RI's available both AZ specific and regional
	// for i := range resp.ReservedInstances {
	// 	ri := resp.ReservedInstances[i]

	// 	// Trim VPC identifier of Platform type as its not relevant for RI Utilization calculations
	// 	platform := strings.TrimSuffix(*ri.ProductDescription, " (Amazon VPC)")

	// 	if *ri.Scope == "Availability Zone" {
	// 		_, ok := az_ri[*ri.AvailabilityZone]
	// 		if !ok {
	// 			az_ri[*ri.AvailabilityZone] = make(map[string]map[string]int)
	// 		}
	// 		_, ok = az_ri[*ri.AvailabilityZone][*ri.InstanceType]
	// 		if !ok {
	// 			az_ri[*ri.AvailabilityZone][*ri.InstanceType] = make(map[string]int)
	// 		}
	// 		az_ri[*ri.AvailabilityZone][*ri.InstanceType][platform] += int(*ri.InstanceCount)
	// 	} else if *ri.Scope == "Region" {
	// 		_, ok := region_ri[*ri.InstanceType]
	// 		if !ok {
	// 			region_ri[*ri.InstanceType] = make(map[string]int)
	// 		}
	// 		region_ri[*ri.InstanceType][platform] += int(*ri.InstanceCount)
	// 	}
	// }

	// // Fetch RI hours used
	// data, err := sendQuery(svcAthena, conf.Athena.DbName, substituteParams(conf.RI.Sql, map[string]string{"**DATE**": date}), region, account)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// // loop through response data and generate map of hourly usage, per AZ, per instance, per platform
	// hours := make(map[string]map[string]map[string]map[string]int)
	// for row := range data.Rows {
	// 	_, ok := hours[data.Rows[row]["date"]]
	// 	if !ok {
	// 		hours[data.Rows[row]["date"]] = make(map[string]map[string]map[string]int)
	// 	}
	// 	_, ok = hours[data.Rows[row]["date"]][data.Rows[row]["az"]]
	// 	if !ok {
	// 		hours[data.Rows[row]["date"]][data.Rows[row]["az"]] = make(map[string]map[string]int)
	// 	}
	// 	_, ok = hours[data.Rows[row]["date"]][data.Rows[row]["az"]][data.Rows[row]["instance"]]
	// 	if !ok {
	// 		hours[data.Rows[row]["date"]][data.Rows[row]["az"]][data.Rows[row]["instance"]] = make(map[string]int)
	// 	}

	// 	v, _ := strconv.ParseInt(data.Rows[row]["hours"], 10, 64)
	// 	hours[data.Rows[row]["date"]][data.Rows[row]["az"]][data.Rows[row]["instance"]][data.Rows[row]["platform"]] += int(v)
	// }

	// // Create new cloudwatch client.
	// svcCloudwatch := cloudwatch.New(sess)

	// // Iterate through each hour and compare the number of instances used vs the number of RIs available
	// // If RI leftover percentage is > 1% push to cloudwatch
	// for hour := range hours {
	// 	if err := riUtilizationHour(svcCloudwatch, hour, hours[hour], az_ri, region_ri, conf, region); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func processCUR(sourceBucket string, reportName string, reportPath string, destPath string, destBucket string, logger *cwlogger.Logger, dateOverride string, curGroups []curgroups.CurGroup) ([]curconvert.CurColumn, string, string, error) {

	var t1 time.Time
	var err error
	if len(dateOverride) == 8 {
		t1, err = time.Parse("20060102", dateOverride)
		if err != nil {
			return nil, "", "", errors.New("Could not parse given date ovrride: " + dateOverride + ", " + err.Error())
		}
	} else {
		t1 = time.Now()
	}

	t1First := time.Date(t1.Year(), t1.Month(), 1, 0, 0, 0, 0, time.Local)

	t2 := t1First.AddDate(0, 1, 0)
	t2First := time.Date(t2.Year(), t2.Month(), 1, 0, 0, 0, 0, time.Local)

	curDate := fmt.Sprintf("%d%02d01-%d%02d01", t1First.Year(), t1First.Month(), t2First.Year(), t2First.Month())
	manifest := reportPath + "/" + curDate + "/" + reportName + "-Manifest.json"

	// Set or extend destPath
	destPathDate := fmt.Sprintf("%d%02d", t1First.Year(), t1First.Month())
	var destPathFull string
	if len(destPath) < 1 {
		destPathFull = "parquet-cur/" + destPathDate
	} else {
		destPathFull = destPath + "/" + destPathDate
	}

	// Init CUR Converter
	cc := curconvert.NewCurConvert(sourceBucket, manifest, destBucket, destPathFull)

	// Check current months manifest exists
	if err := cc.CheckCURExists(); err != nil {
		if err.(awserr.Error).Code() != s3.ErrCodeNoSuchKey {
			return nil, "", "", errors.New("Error fetching CUR Manifest: " + err.Error())
		}
		if t1.Day() > 3 {
			return nil, "", "", errors.New("Error fetching CUR Manifest, NoSuchKey and too delayed: " + err.Error())
		}
		// Regress to processing last months CUR. Error is ErrCodeNoSuchKey and still early in the month
		doLog(logger, "Reseting to previous months CUR for "+reportName)
		t1First = t1First.AddDate(0, 0, -1)
		t2First = t2First.AddDate(0, 0, -1)
		curDate = fmt.Sprintf("%d%02d01-%d%02d01", t1First.Year(), t1First.Month(), t2First.Year(), t2First.Month())
		destPathDate = fmt.Sprintf("%d%02d", t1First.Year(), t1First.Month())

		manifest := reportPath + "/" + curDate + "/" + reportName + "-Manifest.json"
		cc.SetSourceManifest(manifest)

		if len(destPath) < 1 {
			destPathFull = "parquet-cur/" + destPathDate
		} else {
			destPathFull = destPath + "/" + destPathDate
		}
		cc.SetDestPath(destPathFull)
	}

	// If CurGroups exists set them
	if len(curGroups) > 0 {
		cc.SetCurGroups(curGroups...)
	}

	// Convert CUR
	if err := cc.ConvertCur(); err != nil {
		return nil, "", "", errors.New("Could not convert CUR: " + err.Error())
	}

	cols, err := cc.GetCURColumns()
	if err != nil {
		return nil, "", "", errors.New("Could not obtain CUR columns: " + err.Error())
	}
	return cols, "s3://" + destBucket + "/" + destPathFull, destPathDate, nil
}

func createAthenaTable(svcAthena *athena.Athena, dbName string, tablePrefix string, sql string, columns []curconvert.CurColumn, s3Path string, date string, region string, account string) error {

	var cols string
	for col := range columns {
		var t string
		if columns[col].Type == "DECIMAL" {
			t = columns[col].Type + "(" + curconvert.DecimalPrecision + "," + curconvert.DecimalScale + ")"
		} else {
			t = columns[col].Type
		}
		cols += "`" + columns[col].Name + "` " + t + ",\n"
	}
	cols = cols[:strings.LastIndex(cols, ",")]
	sql = substituteParams(sql, map[string]string{"**DBNAME**": dbName, "**PREFIX**": tablePrefix, "**DATE**": date, "**COLUMNS**": cols, "**S3**": s3Path})
	if _, err := sendQuery(svcAthena, dbName, sql, region, account); err != nil {
		return err
	}

	return nil
}

func doLog(logger *cwlogger.Logger, m string) {
	if logger != nil {
		logger.Log(time.Now(), m)
	}
	log.Println(m)
}

func main() {

	/// initialize AWS GO client
	sess := session.Must(session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable}))

	// Grab instance meta-data
	meta := getInstanceMetadata(sess)

	// re-init session now we have the region we are in
	sess = sess.Copy(&aws.Config{Region: aws.String(meta["region"].(string))})

	// Check if running on EC2
	_, ec2 := meta["instanceId"].(string)
	var logger *cwlogger.Logger
	if ec2 { // Init Cloudwatch Logger class if were running on EC2
		logger, err := cwlogger.New(&cwlogger.Config{
			LogGroupName: "CURdashboard",
			Client:       cloudwatchlogs.New(sess),
		})
		if err != nil {
			log.Fatal("Could not initalize Cloudwatch logger: " + err.Error())
		}
		defer logger.Close()
		logger.Log(time.Now(), "CURDashboard running on "+meta["instanceId"].(string)+" in "+meta["availabilityZone"].(string))
	}

	// read in command line params
	var configFile, account, sourceBucket, destBucket, curReportName, curReportPath, curDestPath, dateOverride string
	if err := getParams(&configFile, &sourceBucket, &destBucket, &account, &curReportName, &curReportPath, &curDestPath, &dateOverride); err != nil {
		doLog(logger, err.Error())
		return
	}

	// read in config file
	var conf Config
	if err := getConfig(&conf, configFile); err != nil {
		doLog(logger, err.Error())
	}

	// convert CUR
	columns, s3Path, curDate, err := processCUR(sourceBucket, curReportName, curReportPath, curDestPath, destBucket, logger, dateOverride, conf.CurGroups)
	if err != nil {
		doLog(logger, err.Error())
	}

	// initialize Athena class
	svcAthena := athena.New(sess)
	svcCW := cloudwatch.New(sess)

	// make sure Athena DB exists - dont care about results
	if _, err := sendQuery(svcAthena, "default", conf.Athena.DbSQL, meta["region"].(string), account); err != nil {
		doLog(logger, "Could not create Athena Database: "+err.Error())
	}

	// make sure current Athena tables exists
	tables := make(map[string]string)
	tables[""] = "main"
	for i := range conf.CurGroups {
		tables[conf.CurGroups[i].TablePrefix] = conf.CurGroups[i].PathSuffix
	}

	for tablePrefix, pathPrefix := range tables {
		table := conf.Athena.TablePrefix
		if len(tablePrefix) > 0 {
			table += "_" + tablePrefix
		}
		s3Path := s3Path + "/" + pathPrefix

		if err := createAthenaTable(svcAthena, conf.Athena.DbName, table, conf.Athena.TableSQL, columns, s3Path, curDate, meta["region"].(string), account); err != nil {
			doLog(logger, "Could not create Athena Table: "+err.Error())
		}
	}

	// // If RI analysis enabled - do it
	// if conf.RI.Enabled {
	// 	if err := riUtilization(sess, svcAthena, conf, key, secret, meta["region"].(string), account, date); err != nil {
	// 		doLog(logger, err.Error())
	// 	}
	// }

	// struct for a query job
	type job struct {
		svc      *athena.Athena
		db       string
		account  string
		region   string
		interval string
		metric   Metric
	}

	// channels for parallel execution
	jobs := make(chan job)
	done := make(chan bool)

	// create upto maxConcurrentQueries workers to process metric jobs
	for w := 0; w < maxConcurrentQueries; w++ {
		go func() {
			for {
				j, ok := <-jobs
				if !ok {
					done <- true
					return
				}

				sql := substituteParams(j.metric.SQL, map[string]string{"**DBNAME**": conf.Athena.DbName, "**DATE**": curDate, "**INTERVAL**": conf.MetricConfig.Substring[j.interval]})
				results, err := sendQuery(j.svc, j.db, sql, j.region, j.account)
				if err != nil {
					doLog(logger, "Error querying Athena, SQL: "+sql+" , Error: "+err.Error())
					continue
				}

				if err := sendMetric(svcCW, results, conf.General.Namespace, j.metric.CwName, j.metric.CwType, j.metric.CwDimension, j.interval); err != nil {
					doLog(logger, "Error sending metric, name: "+j.metric.CwName+" , Error: "+err.Error())
				}
			}
		}()
	}

	// pass every enabled metric into channel for processing
	for metric := range conf.Metrics {
		if conf.Metrics[metric].Enabled {
			if conf.Metrics[metric].Hourly {
				jobs <- job{svcAthena, conf.Athena.DbName, account, meta["region"].(string), "hourly", conf.Metrics[metric]}
			}
			if conf.Metrics[metric].Daily {
				jobs <- job{svcAthena, conf.Athena.DbName, account, meta["region"].(string), "daily", conf.Metrics[metric]}
			}
			if conf.Metrics[metric].Monthly {
				jobs <- job{svcAthena, conf.Athena.DbName, account, meta["region"].(string), "monthly", conf.Metrics[metric]}
			}
		}
	}
	close(jobs)

	// wait for jobs to complete
	for w := 0; w < maxConcurrentQueries; w++ {
		<-done
	}
}
