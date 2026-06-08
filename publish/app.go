package main

import (
	"context"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/bain2018/gotask/pkg/gotask"
	"github.com/bain2018/gotask/pkg/mongo_client"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
)

func recordNums(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("gotask goroutine reporter stopped pid=%d ppid=%d err=%v\n", os.Getpid(), os.Getppid(), ctx.Err())
			return
		case <-t.C:
			num := runtime.NumGoroutine()
			log.Printf(" current number %d pid=%d ppid=%d\n", num, os.Getpid(), os.Getppid())
		}
	}
}

func main() {
	startedAt := time.Now()
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("gotask panic pid=%d ppid=%d uptime=%s panic=%v stack=%s\n", os.Getpid(), os.Getppid(), time.Since(startedAt), rec, string(debug.Stack()))
			panic(rec)
		}
	}()

	mongoConfig := mongo_client.LoadConfig()

	ctx, cancel := context.WithTimeout(context.Background(), mongoConfig.ConnectTimeout)
	defer cancel()

	go recordNums(ctx)

	log.Printf(
		"gotask starting pid=%d ppid=%d address=%s mongo_database=%s connect_timeout=%s read_write_timeout=%s mongo_min_pool=%d mongo_max_pool=%d\n",
		os.Getpid(),
		os.Getppid(),
		gotask.GetAddress(),
		mongoConfig.Database,
		mongoConfig.ConnectTimeout,
		mongoConfig.ReadWriteTimeout,
		mongoConfig.MinPoolSize,
		mongoConfig.MaxPoolSize,
	)
	log.Printf(" current address===> %s pid=%d ppid=%d\n", gotask.GetAddress(), os.Getpid(), os.Getppid())

	credential := options.Credential{
		AuthSource:    mongoConfig.AuthSource,
		AuthMechanism: mongoConfig.Mechanism,
		Username:      mongoConfig.Username,
		Password:      mongoConfig.Password,
	}

	opts := options.Client().ApplyURI(mongoConfig.Uri).SetAuth(credential).
		SetReadConcern(readconcern.Majority()).
		SetMaxPoolSize(mongoConfig.MaxPoolSize).
		SetMinPoolSize(mongoConfig.MinPoolSize).
		SetMaxConnIdleTime(60 * time.Second)

	stepStartedAt := time.Now()
	log.Printf("gotask mongo connect start pid=%d ppid=%d\n", os.Getpid(), os.Getppid())
	client, err := mongo.Connect(opts)
	if err != nil {
		log.Printf("gotask mongo connect failed pid=%d ppid=%d duration=%s err=%v\n", os.Getpid(), os.Getppid(), time.Since(stepStartedAt), err)
		log.Fatalln(err)
	}
	log.Printf("gotask mongo connect ok pid=%d ppid=%d duration=%s\n", os.Getpid(), os.Getppid(), time.Since(stepStartedAt))

	stepStartedAt = time.Now()
	log.Printf("gotask mongo ping start pid=%d ppid=%d\n", os.Getpid(), os.Getppid())
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Printf("gotask mongo ping failed pid=%d ppid=%d duration=%s err=%v\n", os.Getpid(), os.Getppid(), time.Since(stepStartedAt), err)
		log.Fatalln(err)
	}
	log.Printf("gotask mongo ping ok pid=%d ppid=%d duration=%s\n", os.Getpid(), os.Getppid(), time.Since(stepStartedAt))

	stepStartedAt = time.Now()
	log.Printf("gotask register start pid=%d ppid=%d\n", os.Getpid(), os.Getppid())
	if err = gotask.Register(mongo_client.NewMongoProxyWithTimeout(client, mongoConfig.ReadWriteTimeout)); err != nil {
		log.Printf("gotask register failed pid=%d ppid=%d duration=%s err=%v\n", os.Getpid(), os.Getppid(), time.Since(stepStartedAt), err)
		log.Fatalln(err)
	}
	log.Printf("gotask register ok pid=%d ppid=%d duration=%s\n", os.Getpid(), os.Getppid(), time.Since(stepStartedAt))

	log.Printf("gotask run start pid=%d ppid=%d address=%s\n", os.Getpid(), os.Getppid(), gotask.GetAddress())
	err = gotask.Run()
	log.Printf("gotask run returned pid=%d ppid=%d uptime=%s err=%v\n", os.Getpid(), os.Getppid(), time.Since(startedAt), err)
	if err != nil {
		log.Fatalln(err)
	}
}
