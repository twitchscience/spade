package gologging

import (
	"fmt"

	"github.com/twitchscience/aws_utils/uploader"
	gen "github.com/twitchscience/gologging/key_name_generator"
)

type UploadLogger struct {
	Uploader *uploader.UploaderPool
	Logger   Logger
}

func StartS3Logger(
	coordinator *RotateCoordinator,
	info *gen.InstanceInfo,
	notifier uploader.NotifierHarness,
	builder uploader.UploaderConstructor,
	errorLogger uploader.ErrorNotifierHarness,
	numWorkers int,
) (*UploadLogger, error) {

	u := uploader.StartUploaderPool(
		numWorkers,
		errorLogger,
		notifier,
		builder,
	)
	log, err := StartS3LogWriter(u, info, coordinator)
	if err != nil {
		return nil, err
	}

	return &UploadLogger{
		Logger:   log,
		Uploader: u,
	}, nil
}

func (u *UploadLogger) Close() {
	u.Logger.Close()
	u.Uploader.Close()
}

func (u *UploadLogger) Log(arg0 string, args ...interface{}) {
	u.Logger.LogWrite(&LogRecord{fmt.Sprintf(arg0+"\n", args...)})
}
