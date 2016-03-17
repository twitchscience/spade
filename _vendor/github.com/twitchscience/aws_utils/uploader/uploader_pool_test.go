package uploader

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type errorCaptureNotifier struct {
	Errors []string
}

type captureNotifier struct {
	receipt []string
}

type testUploader struct {
	req []*UploadRequest
}

type testUploadBuilder struct{}

func (e *errorCaptureNotifier) SendError(err error) {
	e.Errors = append(e.Errors, err.Error())
}

func (c *captureNotifier) SendMessage(r *UploadReceipt) error {
	if strings.Contains(r.Path, "notifyerror") {
		return errors.New(r.Path)
	}
	c.receipt = append(c.receipt, r.Path)
	return nil
}

func (t *testUploader) GetKeyName(filename string) string {
	return filename
}

func (t *testUploader) TargetLocation() string {
	return "test"
}

func (t *testUploader) Upload(req *UploadRequest) (*UploadReceipt, error) {
	if strings.Contains(req.Filename, "uploaderror") {
		return nil, errors.New(req.Filename)
	}

	t.req = append(t.req, req)
	return &UploadReceipt{
		Path:    req.Filename,
		KeyName: t.GetKeyName(req.Filename),
	}, nil
}

func (t *testUploadBuilder) BuildUploader() Uploader {
	return &testUploader{}
}

func TestUploaderPool(t *testing.T) {
	testPool := StartUploaderPool(
		2,
		&errorCaptureNotifier{},
		&captureNotifier{},
		&testUploadBuilder{},
	)
	requests := []*UploadRequest{
		&UploadRequest{
			Filename: "uploaderror1",
			FileType: Gzip,
		},
		&UploadRequest{
			Filename: "test1",
			FileType: Gzip,
		},
		&UploadRequest{
			Filename: "test2",
			FileType: Gzip,
		},
		&UploadRequest{
			Filename: "uploaderror2",
			FileType: Gzip,
		},
		&UploadRequest{
			Filename: "notifyerror3",
			FileType: Gzip,
		},
		&UploadRequest{
			Filename: "test4",
			FileType: Gzip,
		},
	}
	for _, req := range requests {
		testPool.Upload(req)
	}
	testPool.Close()

	//////////////////
	// Test notifier got everything.
	expectedNotifies := []string{
		"test1",
		"test2",
		"test4",
	}
	// Sort to compensate for threads
	sort.Strings(testPool.Notifier.(*captureNotifier).receipt)

	if !reflect.DeepEqual(
		testPool.Notifier.(*captureNotifier).receipt,
		expectedNotifies,
	) {
		t.Errorf("expected %s got %s\n", expectedNotifies,
			testPool.Notifier.(*captureNotifier).receipt,
		)
	}

	///////////////
	// Test Error Notifier got the errors
	expectedErrors := []string{
		"notifyerror3",
		"uploaderror1",
		"uploaderror2",
	}
	// Sort to compensate for threads.
	sort.Strings(testPool.ErrorNotifier.(*errorCaptureNotifier).Errors)
	if !reflect.DeepEqual(
		testPool.ErrorNotifier.(*errorCaptureNotifier).Errors,
		expectedErrors,
	) {
		t.Errorf("expected %s but got %s\n", expectedErrors,
			testPool.ErrorNotifier.(*errorCaptureNotifier).Errors,
		)
	}
}
