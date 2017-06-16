package dynamodoc

import (
	"reflect"

	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"
)

const (
	Equal = "EQ"
)

func (db DB) Insert(table string, v interface{}) error {
	item, err := dynamodbattribute.MarshalMap(v)
	if err != nil {
		return errors.Wrap(err, "dynamodb marshal map questionnaire")
	}

	_, err = db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})

	return err
}

func (db DB) Update(table string, query map[string]interface{}, update interface{}) error {
	q, err := dynamodbattribute.MarshalMap(query)
	if err != nil {
		return errors.Wrap(err, "dynamodbattribute.MarshalMap query")
	}

	u, err := dynamodbattribute.MarshalMap(update)
	if err != nil {
		return errors.Wrap(err, "dynamodbattribute.MarshalMap update")
	}

	delete(u, "id")
	_, err = db.UpdateItem(&dynamodb.UpdateItemInput{
		Key:              q,
		TableName:        aws.String(table),
		AttributeUpdates: AttributeValueToUpdate("PUT", u),
	})

	if err != nil {
		return errors.Wrap(err, "update to dynamodb")
	}

	return nil
}

func (db DB) QueryByIndex(table, k, v, condition string, resultType interface{}, output interface{}) error {
	outputs, err := db.Query(&dynamodb.QueryInput{
		TableName: aws.String(table),
		KeyConditions: map[string]*dynamodb.Condition{
			k: &dynamodb.Condition{
				AttributeValueList: []*dynamodb.AttributeValue{
					{S: aws.String(v)},
				},
				ComparisonOperator: aws.String(condition),
			},
		},
	})

	typ := reflect.TypeOf(resultType).Elem()
	result := reflect.New(typ).Interface()
	results := []interface{}{}
	for _, output := range outputs.Items {
		err = dynamodbattribute.UnmarshalMap(output, &result)
		if err != nil {
			return errors.Wrap(err, "unmarshal each of item")
		}
		results = append(results, result)
	}

	b, err := json.Marshal(results)
	if err != nil {
		return errors.Wrap(err, "marshal results to []byte")
	}

	err = json.Unmarshal(b, output)

	return err
}

func (db DB) FindByID(table, id string, v interface{}) error {
	query := map[string]interface{}{
		"id": id,
	}

	q, err := dynamodbattribute.MarshalMap(query)
	if err != nil {
		return errors.Wrap(err, "dynamodbattribute.MarshalMap query")
	}

	output, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key:       q,
	})

	if err != nil {
		return errors.Wrap(err, "get item")
	}

	return dynamodbattribute.UnmarshalMap(output.Item, v)
}

func AttributeValueToUpdate(action string, item map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValueUpdate {
	update := map[string]*dynamodb.AttributeValueUpdate{}
	for k, v := range item {
		update[k] = &dynamodb.AttributeValueUpdate{
			Action: aws.String(action),
			Value:  v,
		}
	}
	return update
}
