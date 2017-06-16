package dynamodoc

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DB struct {
	*dynamodb.DynamoDB
}

func New(region, url string) DB {
	awscfg := &aws.Config{}
	awscfg.WithRegion(region)
	awscfg.WithEndpoint(url)

	creds := credentials.NewEnvCredentials()
	awscfg.Credentials = creds

	sess := session.Must(session.NewSession(awscfg))
	return DB{dynamodb.New(sess)}
}
