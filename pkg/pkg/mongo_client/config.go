package mongo_client

import (
	"flag"
	"github.com/natefinch/lumberjack"
	"log"
	"os"
	"time"
)

type Config struct {
	Uri              string
	ConnectTimeout   time.Duration
	ReadWriteTimeout time.Duration

	Database    string
	Username    string
	Password    string
	Mechanism   string
	AuthSource  string
	MaxPoolSize uint64
	MinPoolSize uint64
}

var (
	mongoUri         *string
	connectTimeout   *time.Duration
	readWriteTimeout *time.Duration

	database    *string
	username    *string
	password    *string
	mechanism   *string
	authSource  *string
	maxPoolSize *uint64
	minPoolSize *uint64

	logFile  *string
	logLevel *string
)

func getTimeout(env string, fallback time.Duration) (result time.Duration) {
	env, ok := os.LookupEnv(env)
	if !ok {
		return fallback
	}
	result, err := time.ParseDuration(env)
	if err != nil {
		return fallback
	}
	return result
}

// LoadConfig loads Configurations from environmental variables or config file in PHP.
// Environmental variables takes priority.
func LoadConfig() Config {
	if !flag.Parsed() {
		flag.Parse()
	}

	//log config
	log.SetOutput(&lumberjack.Logger{
		Filename:   *logFile,
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     3,     //days
		Compress:   false, // disabled by default
	})

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	return Config{
		Uri:              *mongoUri,
		ConnectTimeout:   *connectTimeout,
		ReadWriteTimeout: *readWriteTimeout,
		Database:         *database,
		Username:         *username,
		Password:         *password,
		Mechanism:        *mechanism,
		AuthSource:       *authSource,
		MaxPoolSize:      *maxPoolSize,
		MinPoolSize:      *minPoolSize,
	}
}
