package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"log"
	"os"
)

func main() {
	h, err := NewHandler()
	if err != nil {
		log.Fatalln(err)
	}
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		lambda.Start(h.HandleLambdaEvent)
	} else {
		if len(os.Args) < 2 {
			log.Fatalln("s3 url is required as an argument")
		}
		err := h.HandleS3URL(os.Args[1])
		if err != nil {
			log.Fatalln(err)
		}
	}
}
