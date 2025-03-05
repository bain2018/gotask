package mongo_client

import (
	"flag"
	"time"
)

func init() {
	parseConfig()
}

func parseConfig() {

	ct := time.Duration(3 * time.Second)
	rwt := time.Duration(60 * time.Second)

	mongoUri = flag.String("uri", "", "the default mongodb uri")
	connectTimeout = flag.Duration("connectTimeout", ct, "mongodb connect timeout")
	readWriteTimeout = flag.Duration("timeout", rwt, "mongodb read write timeout")

	database = flag.String("database", "", "database name to use for the service")
	username = flag.String("username", "", "username to use for the service")
	password = flag.String("password", "", "password to use for the service")
	mechanism = flag.String("mechanism", "", "mechanism to use for the service")
	authSource = flag.String("authSource", "", "auth to use for the service")
	maxPoolSize = flag.Uint64("maxPoolSize", 4, "maximum number of concurrent connections")
	minPoolSize = flag.Uint64("minPoolSize", 1, "minimum number of concurrent connections")
	logFile = flag.String("logFile", "", "log file path")
	logLevel = flag.String("logLevel", "info", "log level")
}
