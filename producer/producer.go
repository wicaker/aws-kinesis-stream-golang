package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/joho/godotenv"
)

var (
	producer AWSKinesis
)

// AWSKinesis struct contain all field needed in kinesis stream
type AWSKinesis struct {
	stream          string
	region          string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

// initiate configuration
func init() {
	e := godotenv.Load() //Load .env file
	if e != nil {
		fmt.Print(e)
	}
	producer = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
}

func main() {
	// connect to aws-kinesis
	s := session.New(&aws.Config{
		Region:      aws.String(producer.region),
		Endpoint:    aws.String(producer.endpoint),
		Credentials: credentials.NewStaticCredentials(producer.accessKeyID, producer.secretAccessKey, producer.sessionToken),
	})
	kc := kinesis.New(s)
	streamName := aws.String(producer.stream)
	_, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})

	//if no stream name in AWS
	if err != nil {
		log.Panic(err)
	}

	// prepare data that will be sent. We use data.json file as example data
	data := openFile()

	// put data to stream
	putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte(data),
		StreamName:   streamName,
		PartitionKey: aws.String("key1"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", *putOutput)
}

// used to open file json
func openFile() string {
	// Open our jsonFile
	jsonFile, err := os.Open("data.json")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened data.json")
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	return string(byteValue)
}
