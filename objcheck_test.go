package objcheck

import (
	"context"
	"fmt"
	"testing"
)

func TestCreateObjList(t *testing.T) {
	ctx := context.Background()
	_, err := createObjList(ctx, 0, 5, "1k")
	if err == nil {
		t.Errorf("Bad pool didn't return error")
	}

	listF, err := createObjList(ctx, 10, 10, "1k")
	if err != nil {
		t.Errorf("Error in obj list creation %v\n", err.Error())
	}
	fmt.Print(listF)
	if len(listF) != 10 {
		t.Errorf("list was %v instead of 10\n", len(listF))
	}
	listL, err := createObjList(ctx, 10000, 10, "1k")
	if err != nil {
		t.Errorf("Error in obj list creation %v\n", err.Error())
	}
	if len(listL) != 10 {
		t.Errorf("list was %v instead of 10\n", len(listL))
	}
	fmt.Print(listL)
}

func TestObjCheckRequestValidate(t *testing.T) {
	ocr := &objCheckRequest{Service: "gcs", Region: "us-east1", Pool: 10, Count: 1}
	err := ocr.validate()
	if err != nil {
		t.Errorf("Unexpected error %v\n", err.Error())
	}

	ocr = &objCheckRequest{Service: "s3", Region: "us-east-2", Pool: 10, Count: 1}
	err = ocr.validate()
	if err != nil {
		t.Errorf("Unexpected error %v\n", err.Error())
	}

	ocr = &objCheckRequest{Service: "gcs", Region: "us-central1", Pool: 10, Count: 1}
	err = ocr.validate()
	if err != nil {
		t.Errorf("Unexpected error %v\n", err.Error())
	}

	ocr = &objCheckRequest{Service: "gcs", Region: "europe-west2", Pool: 10, Count: 1}
	err = ocr.validate()
	if err != nil {
		t.Errorf("Unexpected error %v\n", err.Error())
	}

	ocr = &objCheckRequest{Service: "gcs", Region: "us-central1", Pool: 10, Count: 1000}
	err = ocr.validate()
	if err != nil {
		t.Errorf("Unexpected error %v\n", err.Error())
	}

	ocr = &objCheckRequest{Service: "bb", Region: "us-east1", Pool: 10, Count: 1}
	err = ocr.validate()
	if err == nil {
		t.Error("Missing error for bad service")
	} else if err.Error() != "Bad service bb" {
		t.Errorf("Incorrect error for bad service %v", err.Error())
	}

	ocr = &objCheckRequest{Service: "s3", Region: "us-east1", Pool: 10, Count: 1}
	err = ocr.validate()
	if err == nil {
		t.Error("Missing error for bad service")
	} else if err.Error() != "Bad service / region combination: s3 and us-east1" {
		t.Errorf("Incorrect error for bad service %v", err.Error())
	}

	ocr = &objCheckRequest{Service: "gcs", Region: "us-west-1", Pool: 10, Count: 1}
	err = ocr.validate()
	if err == nil {
		t.Error("Missing error for bad region")
	} else if err.Error() != "Bad region us-west-1" {
		t.Errorf("Incorrect error for bad region %v", err.Error())
	}

	ocr = &objCheckRequest{Service: "gcs", Region: "us-central1", Pool: 99, Count: 1}
	err = ocr.validate()
	if err == nil {
		t.Error("Missing error for bad pool")
	} else if err.Error() != "Bad pool 99" {
		t.Errorf("Incorrect error for bad pool %v", err.Error())
	}

	ocr = &objCheckRequest{Service: "gcs", Region: "us-central1", Pool: 10, Count: -1}
	err = ocr.validate()
	if err == nil {
		t.Error("Missing error for bad count")
	} else if err.Error() != "Bad count" {
		t.Errorf("Incorrect error for bad count %v", err.Error())
	}

	ocr = &objCheckRequest{Service: "gcs", Region: "us-central1", Pool: 10, Count: 10000}
	err = ocr.validate()
	if err == nil {
		t.Error("Missing error for bad count")
	} else if err.Error() != "Bad count" {
		t.Errorf("Incorrect error for bad count %v", err.Error())
	}
}
