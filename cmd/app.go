package main

import (
	"context"
	"log"
	"runtime"
	"time"

	"github.com/bain2018/gotask/pkg/gotask"
	"github.com/bain2018/gotask/pkg/mongo_client"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
)

func recordNums(ctx context.Context) {
	t := time.Tick(30 * time.Second)
	for {
		select {
		case <-t:
			num := runtime.NumGoroutine()
			log.Printf(" current number %d\n", num)
		}
	}
}

func main() {
	mongoConfig := mongo_client.LoadConfig()

	ctx, cancel := context.WithTimeout(context.Background(), mongoConfig.ConnectTimeout)
	defer cancel()

	go recordNums(ctx)

	credential := options.Credential{
		AuthSource:    mongoConfig.AuthSource,
		AuthMechanism: mongoConfig.Mechanism,
		Username:      mongoConfig.Username,
		Password:      mongoConfig.Password,
	}

	log.Printf(" current address===> %s\n", gotask.GetAddress())

	opts := options.Client().ApplyURI(mongoConfig.Uri).
		SetAuth(credential).SetReadConcern(readconcern.Majority()).
		SetMaxPoolSize(mongoConfig.MaxPoolSize).
		SetMinPoolSize(mongoConfig.MaxPoolSize).SetMaxConnIdleTime(60 * time.Second)

	client, err := mongo.Connect(opts)
	if err != nil {
		log.Fatalln(err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalln(err)
	}

	if err = gotask.Register(mongo_client.NewMongoProxyWithTimeout(client, mongoConfig.ReadWriteTimeout)); err != nil {
		log.Fatalln(err)
	}

	if err = gotask.Run(); err != nil {
		log.Fatalln(err)
	}
}
