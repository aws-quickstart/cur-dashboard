package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/urfave/cli"
)

type Map struct {
	Value string   `json:"value"`
	Match []string `json:"match"`
	Regex []string `json:"regex"`
}

type TagMap struct {
	Tags []string `json:"tags"`
	Map  []Map    `json:"map"`
	Name string   `json:"name"`
}

type Config struct {
	TagMap       []TagMap            `json:"tagmap"`
	TagBlacklist map[string][]string `json:"tagblacklist"`
	Sql          map[string]string   `json:"sql"`
	Tags         string
	TagsGroup    string
	Database     string
	Table        string
	Account      string
}

type AthenaResponse struct {
	Rows []map[string]string
}

type Results struct {
	tagCosts map[string]float64
	total    float64
}

var RegexCache map[string]*regexp.Regexp

func substituteParams(sql string, params map[string]string) string {

	for sub, value := range params {
		sql = strings.Replace(sql, sub, value, -1)
	}

	return sql
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
	if err := json.Unmarshal(b, &conf); err != nil {
		return errors.New("Error Decoding config file: " + err.Error())
	}

	return nil
}

func getCreds(arn string, externalID string, mfa string, sess *session.Session) *credentials.Credentials {
	if len(arn) < 1 {
		return nil
	}
	if len(mfa) > 0 {
		return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {
			p.SerialNumber = aws.String(mfa)
			p.TokenProvider = stscreds.StdinTokenProvider
			if len(externalID) > 0 {
				p.ExternalID = aws.String(externalID)
			}
		})
	}
	if len(externalID) > 0 {
		return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {
			p.ExternalID = aws.String(externalID)
		})
	}
	return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {})
}

/*
Function takes SQL to send to Athena converts into JSON to send to Athena HTTP proxy and then sends it.
Then recieves responses in JSON which is converted back into a struct and returned
*/
func sendQuery(svc *athena.Athena, db string, sql string, account string, region string, s3ResultsLocation string) (AthenaResponse, error) {
	var results AthenaResponse
	var s athena.StartQueryExecutionInput
	s.SetQueryString(sql)

	var q athena.QueryExecutionContext
	q.SetDatabase(db)
	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	if len(s3ResultsLocation) > 1 {
		r.SetOutputLocation(s3ResultsLocation)
	} else {
		r.SetOutputLocation("s3://aws-athena-query-results-" + account + "-" + region + "/")
	}
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
		return results, fmt.Errorf("Error Querying Athena, query state is: %s, detailed error %s", *qrop.QueryExecution.Status.State, *qrop.QueryExecution.Status.StateChangeReason)
	}

	var ip athena.GetQueryResultsInput
	ip.SetQueryExecutionId(*result.QueryExecutionId)
	ip.SetMaxResults(1000)

	// loop through results (paginated call)
	var colNames []string
	i := 1
	err = svc.GetQueryResultsPages(&ip,
		func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
			i++
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

func findExact(value string, list []string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

func findRegex(value string, list []string) bool {

	if RegexCache == nil {
		RegexCache = make(map[string]*regexp.Regexp)
	}

	for _, v := range list {
		r, ok := RegexCache[v]
		if !ok {
			var err error
			r, err = regexp.Compile(v)
			if err != nil {
				fmt.Println("Regex: " + v + ", invalid - skipping")
				continue
			}
			RegexCache[v] = r
		}
		if r.MatchString(value) {
			return true
		}
	}
	return false
}

func findTagMatch(match string, m []Map, tag string, blacklist map[string][]string) (string, error) {
	for _, object := range m {
		if findExact(match, object.Match) {
			return object.Value, nil
		}
	}

	for _, object := range m {
		if findRegex(match, object.Regex) {
			return object.Value, nil
		}
	}

	tagblacklist, ok := blacklist[tag]
	if ok {
		if findRegex(match, tagblacklist) {
			return "", fmt.Errorf("No Match")
		}
	}
	if len(match) > 0 {
		return match, nil
	}

	return "", fmt.Errorf("No Match")
}

func processResults(resp AthenaResponse, c Config) Results {

	r := &Results{
		tagCosts: make(map[string]float64),
		total:    0,
	}

	for _, row := range resp.Rows {
		f, err := strconv.ParseFloat(row["cost"], 64)
		if err != nil {
			fmt.Println("Failed to convert float, continuing")
			continue
		}

		tags := []string{row["service"]}
		for _, tm := range c.TagMap {
			found := false
			for i := range tm.Tags {
				match, err := findTagMatch(row[tm.Tags[i]], tm.Map, tm.Tags[i], c.TagBlacklist)
				if err == nil {
					tags = append(tags, match)
					found = true
					break
				}
			}
			if !found {
				tags = append(tags, "Untagged")
			}
		}
		r.tagCosts[strings.Join(tags, ",")] += f
		r.total += f
	}
	return *r
}

func processRIUsage(conf Config, svcAthena *athena.Athena, region string, s3ResultsLocation string, tagCost AthenaResponse) (AthenaResponse, error) {
	// Total RI Cost
	sql := substituteParams(conf.Sql["ricost"], map[string]string{"**DB**": conf.Database, "**TABLE**": conf.Table})
	riCost, err := sendQuery(svcAthena, conf.Database, sql, conf.Account, region, s3ResultsLocation)
	if err != nil {
		return tagCost, err
	}
	riCostPerService := make(map[string]float64)
	for _, row := range riCost.Rows {
		f, err := strconv.ParseFloat(row["cost"], 64)
		if err != nil {
			fmt.Println("Failed to convert float, continuing")
			continue
		}
		riCostPerService[row["service"]] = f
	}

	// RI Usage Per tag
	var riUsage AthenaResponse
	sql = substituteParams(conf.Sql["riusage"], map[string]string{"**TAGS**": conf.Tags, "**TAGSGROUP**": conf.TagsGroup, "**DB**": conf.Database, "**TABLE**": conf.Table})
	riUsage, err = sendQuery(svcAthena, conf.Database, sql, conf.Account, region, s3ResultsLocation)
	if err != nil {
		return tagCost, err
	}

	// Total RI Usage per Service
	riUsagePerService := make(map[string]float64)
	for _, row := range riUsage.Rows {
		f, err := strconv.ParseFloat(row["normalized_amount"], 64)
		if err == nil && f > 0 {
			riUsagePerService[row["service"]] += f
		} else {
			f, err := strconv.ParseFloat(row["amount"], 64)
			if err == nil && f > 0 {
				riUsagePerService[row["service"]] += f
			}
		}
	}

	// Calculate individual RI cost per row and append to riCost
	for _, row := range riUsage.Rows {
		f, err := strconv.ParseFloat(row["normalized_amount"], 64)
		if err == nil && f > 0 {
			cost := (f / riUsagePerService[row["service"]]) * riCostPerService[row["service"]]
			row["cost"] = strconv.FormatFloat(cost, 'E', -1, 64)
		} else {
			f, err := strconv.ParseFloat(row["amount"], 64)
			if err == nil && f > 0 {
				cost := (f / riUsagePerService[row["service"]]) * riCostPerService[row["service"]]
				row["cost"] = strconv.FormatFloat(cost, 'E', -1, 64)
			}
		}
		tagCost.Rows = append(tagCost.Rows, row)
	}
	return tagCost, nil
}

func printResults(r Results, c Config) {

	var keys []string
	for k := range r.tagCosts {
		keys = append(keys, k)
	}

	var tagNames string
	for _, v := range c.TagMap {
		tagNames += "\"" + v.Name + "\","
	}

	sort.Strings(keys)

	fmt.Println("\"service\"," + tagNames + "\"amount\"")
	for _, k := range keys {
		if math.Round(r.tagCosts[k]/0.01)*0.01 > 0.01 {
			fmt.Printf("%s,%.2f\n", k, math.Round(r.tagCosts[k]/0.01)*0.01)
		}
	}
	fmt.Println("---------------------")
	fmt.Printf("Total: %.2f", math.Round(r.total/0.01)*0.01)
}

func addTagCase(tag string, blacklist map[string][]string, appendTagName bool) string {

	if _, ok := blacklist[tag]; !ok {
		return "\"" + tag + "\""
	}

	sql := "CASE "
	for i := range blacklist[tag] {
		sql += "WHEN regexp_like(\"" + tag + "\", '" + blacklist[tag][i] + "*') THEN '' "
	}
	sql += "ELSE \"" + tag + "\" END"
	if appendTagName {
		sql += " as \"" + tag + "\""
	}
	return sql
}

func main() {
	app := cli.NewApp()
	app.Name = "Cost CLI"
	app.Usage = "Command Line Interface for download, conversion and re-upload of the AWS CUR from/to a S3 Bucket."
	app.Version = "1.0.0"

	var startDate, endDate, database, table, region, roleArn, externalID, configFile, s3ResultsLocation, mfa string
	var riUsage bool
	app.Commands = []cli.Command{
		{
			Name:  "costbytag",
			Usage: "Perform CUR Conversion",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "startDate, sd",
					Usage:       "Date in YYYMMDD format",
					Value:       time.Now().Format("201601") + "01",
					Destination: &startDate,
				},
				cli.StringFlag{
					Name:        "endDate, ed",
					Usage:       "Date in YYYMMDD format",
					Value:       time.Now().Format("201601") + "31",
					Destination: &endDate,
				},
				cli.StringFlag{
					Name:        "database, db",
					Usage:       "Athena Database to use",
					Value:       "cur",
					Destination: &database,
				},
				cli.StringFlag{
					Name:        "table, tb",
					Usage:       "Athena Table to use",
					Value:       "",
					Destination: &table,
				},
				cli.StringFlag{
					Name:        "mfaSerial, mfa",
					Usage:       "Optional MFA Serial or ARN",
					Value:       "",
					Destination: &mfa,
				},
				cli.StringFlag{
					Name:        "resultsLocation, rl",
					Usage:       "Athena Results Location override",
					Value:       "",
					Destination: &s3ResultsLocation,
				},
				cli.StringFlag{
					Name:        "region, r",
					Usage:       "AWS Region Athena Database and Table exist in (default us-east-1)",
					Value:       "us-east-1",
					Destination: &region,
				},
				cli.StringFlag{
					Name:        "roleArn, arn",
					Usage:       "Optional role ARN to assume when querying Athena",
					Value:       "",
					Destination: &roleArn,
				},
				cli.StringFlag{
					Name:        "externalID, extid",
					Usage:       "Optional role ARN to assume when querying Athena",
					Value:       "",
					Destination: &externalID,
				},
				cli.StringFlag{
					Name:        "config, c",
					Usage:       "JSON tag configuration",
					Value:       "",
					Destination: &configFile,
				},
				cli.BoolFlag{
					Name:        "riusage, ri",
					Usage:       "Process RI Usage and append to results",
					Destination: &riUsage,
				},
			},
			Action: func(c *cli.Context) error {

				if len(table) < 1 {
					cli.ShowCommandHelp(c, "costbytag")
					log.Fatalln("Must supply a Athena Table to query")
				}

				// read in config file
				var conf Config
				if err := getConfig(&conf, configFile); err != nil {
					return err
				}
				conf.Database = database
				conf.Table = table

				sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
				if err != nil {
					return err
				}

				// if needed set creds for AssumeRole and reset session
				if len(roleArn) > 0 {
					sess = sess.Copy(&aws.Config{Credentials: getCreds(roleArn, externalID, mfa, sess)})
				}

				// fetch account ID
				svc := sts.New(sess)
				result, err := svc.GetCallerIdentity(&sts.GetCallerIdentityInput{})
				if err != nil {
					return err
				}
				conf.Account = *result.Account

				for _, tm := range conf.TagMap {
					for i := range tm.Tags {
						conf.Tags += addTagCase(tm.Tags[i], conf.TagBlacklist, true) + ","
						conf.TagsGroup += addTagCase(tm.Tags[i], conf.TagBlacklist, false) + ","
					}
				}
				conf.Tags = conf.Tags[:len(conf.Tags)-1]
				conf.TagsGroup = conf.TagsGroup[:len(conf.TagsGroup)-1]

				// Normal Cost per tag
				svcAthena := athena.New(sess)
				sql := conf.Sql["tagmap"]
				if riUsage {
					sql = conf.Sql["tagmapri"]
				}
				sql = substituteParams(sql, map[string]string{"**TAGS**": conf.Tags, "**TAGSGROUP**": conf.TagsGroup, "**DB**": conf.Database, "**TABLE**": conf.Table})
				tagCost, err := sendQuery(svcAthena, conf.Database, sql, conf.Account, region, s3ResultsLocation)
				if err != nil {
					return err
				}

				if riUsage {
					tagCost, err = processRIUsage(conf, svcAthena, region, s3ResultsLocation, tagCost)
					if err != nil {
						return fmt.Errorf("Could not process RI information - try again or remove flag. Error: %s", err.Error())
					}
				}
				printResults(processResults(tagCost, conf), conf)

				return nil
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}
