package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/joho/godotenv"
)

var (
	kinesisStream AWSKinesis
)

// AWSKinesis struct , the collection of all field will be needed in kinesis stream
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
	kinesisStream = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
}

func main() {
	action := flag.String("action", "create", "choose question `create` or `delete`")
	flag.Parse()

	// connect to aws-kinesis
	s := session.New(&aws.Config{
		Region:      aws.String(kinesisStream.region),
		Endpoint:    aws.String(kinesisStream.endpoint),
		Credentials: credentials.NewStaticCredentials(kinesisStream.accessKeyID, kinesisStream.secretAccessKey, kinesisStream.sessionToken),
	})
	kc := kinesis.New(s)
	streamName := aws.String(kinesisStream.stream)

	// create or delete kinesis stream name
	if *action == "create" {
		out, err := kc.CreateStream(&kinesis.CreateStreamInput{
			ShardCount: aws.Int64(1),
			StreamName: streamName,
		})
		if err != nil {
			log.Panic(err)
		}
		fmt.Printf("%v\n", out)

		if err := kc.WaitUntilStreamExists(&kinesis.DescribeStreamInput{StreamName: streamName}); err != nil {
			log.Panic(err)
		}
		log.Println("StreamName successfully created")
	} else if *action == "delete" {
		deleteOutput, err := kc.DeleteStream(&kinesis.DeleteStreamInput{
			StreamName: streamName,
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("Delete successfully %v\n", deleteOutput)
	} else {
		fmt.Println("Wrong input")
	}
}
