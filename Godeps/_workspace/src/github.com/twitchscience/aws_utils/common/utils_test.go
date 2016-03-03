package common

import (
	"errors"
	"testing"
)

func TestRetrier(t *testing.T) {
	if testing.Short() {
		t.Skip("Takes 14 seconds, so don't run during short mode")
	}

	retrier := &Retrier{
		Times:         3,
		BackoffFactor: 2,
	}
	timesCalled := 0
	failOnce := func() error {
		timesCalled++
		if timesCalled < 2 {
			return errors.New("")
		}
		return nil
	}
	err := retrier.Retry(failOnce)
	if err != nil {
		t.Errorf("expected to not have error\n", "retier.Retry(failOnce)")
	}
	if timesCalled != 2 {
		t.Errorf("expected %s to call %d\n", "retier.Retry(failOnce)", 2)
	}

	timesCalled = 0
	failThreeTimes := func() error {
		timesCalled++
		if timesCalled < 4 {
			return errors.New("")
		}
		return nil
	}
	err = retrier.Retry(failThreeTimes)
	if err == nil {
		t.Errorf("expected to have error\n", "retier.Retry(failOnce)")
	}
	if timesCalled != 3 {
		t.Errorf("expected %s to call %d\n", "retier.Retry(failOnce)", 2)
	}

}

func TestNormalizeS3URL(t *testing.T) {
	if "s3://foo" != NormalizeS3URL("s3://foo") {
		t.Fatal("Failed to handle url with s3:// prefix")
	}
	if "s3://foo" != NormalizeS3URL("foo") {
		t.Fatal("Failed to handle url without s3:// prefix")
	}
}
