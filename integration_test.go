package dynamodoc

import (
	"fmt"
	"testing"
)

type Answer struct {
	ID              string            `json:"id"`
	QuestionnaireID string            `json:"questionnaire"`
	Answers         map[string]string `json:"answers"`
}

func TestQueryByIndex(t *testing.T) {
	var answer Answer
	var answers []Answer
	db := New("ap-southeast-1", "https://dynamodb.ap-southeast-1.amazonaws.com")
	err := db.QueryByIndex("answers", "questionnaire", "c177f4de-d0b5-45fa-b692-9fbbae901c70", Equal, &answer, &answers)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%#v\n", answers)
}
