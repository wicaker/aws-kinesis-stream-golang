package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/joho/godotenv"
)

var (
	consumer AWSKinesis
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
	consumer = AWSKinesis{
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
		Region:      aws.String(consumer.region),
		Endpoint:    aws.String(consumer.endpoint),
		Credentials: credentials.NewStaticCredentials(consumer.accessKeyID, consumer.secretAccessKey, consumer.sessionToken),
	})
	kc := kinesis.New(s)
	streamName := aws.String(consumer.stream)
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		log.Panic(err)
	}

	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard Id is provided when making put record(s) request.
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		// ShardIteratorType: aws.String("AT_SEQUENCE_NUMBER"),
		// ShardIteratorType: aws.String("LATEST"),
		StreamName: streamName,
	})
	if err != nil {
		log.Panic(err)
	}

	shardIterator := iteratorOutput.ShardIterator
	var a *string

	// get data using infinity looping
	// we will attempt to consume data every 1 secons, if no data, nothing will be happen
	for {
		// get records use shard iterator for making request
		records, err := kc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})

		// if error, wait until 1 seconds and continue the looping process
		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		// process the data
		if len(records.Records) > 0 {
			for _, d := range records.Records {
				m := make(map[string]interface{})
				err := json.Unmarshal([]byte(d.Data), &m)
				if err != nil {
					log.Println(err)
					continue
				}
				log.Printf("GetRecords Data: %v\n", m)
			}
		} else if records.NextShardIterator == a || shardIterator == records.NextShardIterator || err != nil {
			log.Printf("GetRecords ERROR: %v\n", err)
			break
		}
		shardIterator = records.NextShardIterator
		time.Sleep(1000 * time.Millisecond)
	}
}
