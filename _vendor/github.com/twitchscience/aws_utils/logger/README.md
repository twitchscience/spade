# logger
logger is a library for writing structured json logs to stderr.

It is a wrapper around [logrus](https://github.com/Sirupsen/logrus) that
automatically formats as JSON and provides some default context
(env, pid, host, and caller). It  can capture output written to the default logger
(since external dependencies will not be using this logger), and can add a rollbar 
rollbar [hook](https://github.com/Sirupsen/logrus#hooks) to send all 
errors/panics/fatals to rollbar.

## Usage
To use logger, just import this library, then call `logger.Init(<level>)`
 from your main or init.
Then, you can log output using functions like `logger.Infoln("message")` and
`logger.Fatal("ohno")`. See the
[logrus](https://github.com/Sirupsen/logrus) documentation for more functions you can
use.

To use the rollbar logger, initialize with:
```
logger.InitWithRollbar(<level>, <token>, <env>)
```
Add this to main to send all top level panics to rollbar:
```
defer logger.LogPanic()
```

To capture and log panics in other goroutines, spawn them with
```
logger.Go( <func> )
```

And in your shutdown code, include `logger.Wait()` to wait for any remaining
logs to be sent to rollbar before exiting.

## Capturing default logger output
To capture output from the default logger, use `logger.CaptureDefault()`.
To create a golang logger that will have its output forwarded to logger, use
`logger.GetCapturedLogger()`.
If you do these, don't provide additional configuration to the captured logger,
or you may break the capturing.
