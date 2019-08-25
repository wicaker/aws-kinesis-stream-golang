# AWS Kinesis Data Stream With GoLang
This repositories was created to provide the way to send and get data to and from aws kinesis with go programming langange (golang). We will using `kinesis-local` docker image to get simulation of aws kinesis in local computer. 

This project was devided to be three sub folder : 
- `consumer` to consume data from kinesis
- `producer` to product data to kinesis
- `stream` to create or delete streaming name

## How to run ?
Build:
- `docker build -t kinesis-local .` or you can use docker image in https://hub.docker.com/r/ruanbekker/kinesis-local

Run and expose port 4567:
- `docker run -it -p 4567:4567 kinesis-local:latest`

Create or delete stream name :
- Move to `stream` folder to create or delete stream name
- `go build stream.go `
- create : `./stream -action=create`
- delete : `./stream -action=delete`

Produce and consume data (split the terminal to be 2 part, so you can see simulation of data stream):
- Move to producer folder to produce/send data to kinesis and `go run produce.go`
- Move to consumer folder to consume/get data from kinesis and `go run consume.go`

GOOD LUCK :)