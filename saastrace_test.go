package saastrace

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
