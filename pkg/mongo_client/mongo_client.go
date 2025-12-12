package mongo_client

import (
	"context"
	"time"

	"github.com/bain2018/gotask/pkg/gotask"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoProxy struct {
	timeout time.Duration
	client  *mongo.Client
}

// NewMongoProxy creates a new Mongo Proxy
func NewMongoProxy(client *mongo.Client) *MongoProxy {
	return &MongoProxy{
		5 * time.Second,
		client,
	}
}

// NewMongoProxyWithTimeout creates a new Mongo Proxy, with a read write timeout.
func NewMongoProxyWithTimeout(client *mongo.Client, timeout time.Duration) *MongoProxy {
	return &MongoProxy{
		timeout,
		client,
	}
}

func (m *MongoProxy) getHandler(i interface{}, f coreExecutor) gotask.Handler {
	mw := stackMiddleware(i)
	return mw(func(cmd interface{}, result *interface{}) error {
		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		defer cancel()
		return f(ctx, result)
	})
}

func (m *MongoProxy) exec(i interface{}, payload []byte, result *[]byte, f coreExecutor) error {
	var r interface{}
	defer func() {
		if r == nil {
			*result = nil
		} else {
			*result = r.([]byte)
		}
	}()
	return m.getHandler(i, f)(payload, &r)
}

type coreExecutor func(ctx context.Context, r *interface{}) error

type InsertOneCmd struct {
	Database   string
	Collection string
	Record     bson.Raw
	Opts       *options.InsertOneOptions
	OptsList   options.Lister[options.InsertOneOptions]
}

// InsertOne executes an insert command to insert a single document into the collection.
func (m *MongoProxy) InsertOne(payload []byte, result *[]byte) (err error) {
	cmd := &InsertOneCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) error {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)
		opts := options.InsertOne()

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		*r, err = collection.InsertOne(ctx, cmd.Record, opts)
		return err
	})
}

type InsertManyCmd struct {
	Database   string
	Collection string
	Records    []interface{}
	Opts       *options.InsertManyOptions
	OptsList   options.Lister[options.InsertManyOptions]
}

// InsertMany executes an insert command to insert multiple documents into the collection. If write errors occur
// during the operation (e.g. duplicate key error), this method returns a BulkWriteException error.
func (m *MongoProxy) InsertMany(payload []byte, result *[]byte) (err error) {
	cmd := &InsertManyCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) error {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.InsertMany()

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		if cmd.Opts.Ordered != nil {
			opts.SetOrdered(*cmd.Opts.Ordered)
		}

		*r, err = collection.InsertMany(ctx, cmd.Records, opts)
		return err
	})
}

type FindOneCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Opts       *options.FindOneOptions
	OptsList   options.Lister[options.FindOneOptions]
}

// FindOne executes a find command and returns one document in the collection.
func (m *MongoProxy) FindOne(payload []byte, result *[]byte) error {
	cmd := &FindOneCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.FindOne()
		if cmd.Opts.AllowPartialResults != nil {
			opts.SetAllowPartialResults(*cmd.Opts.AllowPartialResults)
		}

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}
		//
		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}
		//
		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}
		//
		if cmd.Opts.Max != nil {
			opts.SetMax(cmd.Opts.Max)
		}
		//
		if cmd.Opts.Min != nil {
			opts.SetMin(cmd.Opts.Min)
		}
		//
		if cmd.Opts.OplogReplay != nil {
			//opts.SetOplogReplay(*cmd.Opts.OplogReplay)
		}
		//
		if cmd.Opts.Projection != nil {
			opts.SetProjection(cmd.Opts.Projection)
		}
		//
		if cmd.Opts.ReturnKey != nil {
			opts.SetReturnKey(*cmd.Opts.ReturnKey)
		}
		//
		if cmd.Opts.ShowRecordID != nil {
			opts.SetShowRecordID(*cmd.Opts.ShowRecordID)
		}
		//
		if cmd.Opts.Sort != nil {
			opts.SetSort(cmd.Opts.Sort)
		}
		//
		if cmd.Opts.Skip != nil {
			opts.SetSkip(*cmd.Opts.Skip)
		}

		err = collection.FindOne(ctx, cmd.Filter, opts).Decode(r)
		return err
	})
}

type FindOneAndDeleteCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Opts       *options.FindOneAndDeleteOptions
	OptsList   options.Lister[options.FindOneAndDeleteOptions]
}

// FindOneAndDelete executes a findAndModify command to delete at most one document in the collection. and returns the
// document as it appeared before deletion.
func (m *MongoProxy) FindOneAndDelete(payload []byte, result *[]byte) error {
	cmd := &FindOneAndDeleteCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.FindOneAndDelete()

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Projection != nil {
			opts.SetProjection(cmd.Opts.Projection)
		}

		if cmd.Opts.Sort != nil {
			opts.SetSort(cmd.Opts.Sort)
		}
		//
		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		err = collection.FindOneAndDelete(ctx, cmd.Filter, opts).Decode(r)
		return err
	})
}

type FindOneAndUpdateCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Update     bson.Raw
	Opts       *options.FindOneAndUpdateOptions
	OptsList   options.Lister[options.FindOneAndUpdateOptions]
}

// FindOneAndUpdate executes a findAndModify command to update at most one document in the collection and returns the
// document as it appeared before updating.
func (m *MongoProxy) FindOneAndUpdate(payload []byte, result *[]byte) error {
	cmd := &FindOneAndUpdateCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.FindOneAndUpdate()

		if len(cmd.Opts.ArrayFilters) > 0 {
			opts.SetArrayFilters(cmd.Opts.ArrayFilters)
		}

		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Projection != nil {
			opts.SetProjection(cmd.Opts.Projection)
		}

		if cmd.Opts.Sort != nil {
			opts.SetSort(cmd.Opts.Sort)
		}
		//
		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		if cmd.Opts.ReturnDocument != nil {
			opts.SetReturnDocument(*cmd.Opts.ReturnDocument)
		}

		if cmd.Opts.Upsert != nil {
			opts.SetUpsert(*cmd.Opts.Upsert)
		}

		err = collection.FindOneAndUpdate(ctx, cmd.Filter, cmd.Update, opts).Decode(r)
		return err
	})
}

type FindOneAndReplaceCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Replace    bson.Raw
	Opts       *options.FindOneAndReplaceOptions
	OptsList   options.Lister[options.FindOneAndReplaceOptions]
}

// FindOneAndReplace executes a findAndModify command to replace at most one document in the collection
// and returns the document as it appeared before replacement.
func (m *MongoProxy) FindOneAndReplace(payload []byte, result *[]byte) error {
	cmd := &FindOneAndReplaceCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.FindOneAndReplace()

		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Projection != nil {
			opts.SetProjection(cmd.Opts.Projection)
		}

		if cmd.Opts.ReturnDocument != nil {
			opts.SetReturnDocument(*cmd.Opts.ReturnDocument)
		}

		if cmd.Opts.Sort != nil {
			opts.SetSort(cmd.Opts.Sort)
		}

		if cmd.Opts.Upsert != nil {
			opts.SetUpsert(*cmd.Opts.Upsert)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		err = collection.FindOneAndReplace(ctx, cmd.Filter, cmd.Replace, cmd.OptsList).Decode(r)
		return err
	})
}

type FindCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Opts       *options.FindOptions
	OptsList   options.Lister[options.FindOptions]
}

// Find executes a find command and returns all the matching documents in the collection.
func (m *MongoProxy) Find(payload []byte, result *[]byte) error {
	cmd := &FindCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) error {
		var rr []interface{}
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		builder := options.Find()

		builder.SetCollation(cmd.Opts.Collation)
		builder.SetComment(cmd.Opts.Comment)
		builder.SetHint(cmd.Opts.Hint)

		builder.SetMax(cmd.Opts.Max)

		if cmd.Opts.MaxAwaitTime != nil {
			builder.SetMaxAwaitTime(*cmd.Opts.MaxAwaitTime)
		}
		builder.SetMin(cmd.Opts.Min)
		//builder.SetOplogReplay()
		builder.SetProjection(cmd.Opts.Projection)
		if cmd.Opts.ReturnKey != nil {
			builder.SetReturnKey(*cmd.Opts.ReturnKey)
		}

		if cmd.Opts.ShowRecordID != nil {
			builder.SetShowRecordID(*cmd.Opts.ShowRecordID)
		}

		if cmd.Opts.Skip != nil {
			builder.SetSkip(*cmd.Opts.Skip)
		}

		builder.SetSort(cmd.Opts.Sort)
		if cmd.Opts.AllowDiskUse != nil {
			builder.SetAllowDiskUse(*cmd.Opts.AllowDiskUse)
		}

		if cmd.Opts.BatchSize != nil {
			builder.SetBatchSize(*cmd.Opts.BatchSize)
		}

		if cmd.Opts.CursorType != nil {
			builder.SetCursorType(*cmd.Opts.CursorType)
		}

		builder.SetLet(cmd.Opts.Let)
		if cmd.Opts.Limit != nil {
			builder.SetLimit(*cmd.Opts.Limit)
		}

		if cmd.Opts.NoCursorTimeout != nil {
			builder.SetNoCursorTimeout(*cmd.Opts.NoCursorTimeout)
		}

		if cmd.Opts.AllowPartialResults != nil {
			builder.SetAllowPartialResults(*cmd.Opts.AllowPartialResults)
		}

		if cmd.Opts.Limit != nil {
			builder.SetLimit(*cmd.Opts.Limit)
		}

		cursor, err := collection.Find(ctx, cmd.Filter, builder)
		if cursor != nil {
			defer cursor.Close(ctx)
			err = cursor.All(ctx, &rr)
		}
		*r = rr
		return err
	})
}

type UpdateOneCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Update     bson.Raw
	Opts       *options.UpdateOneOptions
	OptsList   options.Lister[options.UpdateOneOptions]
}

// UpdateOne executes an update command to update at most one document in the collection.
func (m *MongoProxy) UpdateOne(payload []byte, result *[]byte) error {
	cmd := &UpdateOneCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.UpdateOne()

		if len(cmd.Opts.ArrayFilters) > 0 {
			opts.SetArrayFilters(cmd.Opts.ArrayFilters)
		}

		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Upsert != nil {
			opts.SetUpsert(*cmd.Opts.Upsert)
		}

		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		if cmd.Opts.Sort != nil {
			opts.SetSort(cmd.Opts.Sort)
		}

		*r, err = collection.UpdateOne(ctx, cmd.Filter, cmd.Update, opts)
		return err
	})
}

type UpdateManyCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Update     bson.Raw
	Opts       *options.UpdateManyOptions
	OptsList   options.Lister[options.UpdateManyOptions]
}

// UpdateMany executes an update command to update documents in the collection.
func (m *MongoProxy) UpdateMany(payload []byte, result *[]byte) error {
	cmd := &UpdateManyCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.UpdateMany()

		if len(cmd.Opts.ArrayFilters) > 0 {
			opts.SetArrayFilters(cmd.Opts.ArrayFilters)
		}

		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Upsert != nil {
			opts.SetUpsert(*cmd.Opts.Upsert)
		}

		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		*r, err = collection.UpdateMany(ctx, cmd.Filter, cmd.Update, opts)
		return err
	})
}

type ReplaceOneCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Replace    bson.Raw
	Opts       *options.ReplaceOptions
	OptsList   options.Lister[options.ReplaceOptions]
}

// ReplaceOne executes an update command to replace at most one document in the collection.
func (m *MongoProxy) ReplaceOne(payload []byte, result *[]byte) error {
	cmd := &ReplaceOneCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.Replace()
		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Upsert != nil {
			opts.SetUpsert(*cmd.Opts.Upsert)
		}

		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		if cmd.Opts.Sort != nil {
			opts.SetSort(cmd.Opts.Sort)
		}

		*r, err = collection.ReplaceOne(ctx, cmd.Filter, cmd.Replace, cmd.OptsList)
		return err
	})
}

type CountDocumentsCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Opts       *options.CountOptions
	OptsList   options.Lister[options.CountOptions]
}

// CountDocuments returns the number of documents in the collection.
func (m *MongoProxy) CountDocuments(payload []byte, result *[]byte) error {
	cmd := &CountDocumentsCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.Count()

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Skip != nil {
			opts.SetSkip(*cmd.Opts.Skip)
		}

		if cmd.Opts.Limit != nil {
			opts.SetLimit(*cmd.Opts.Limit)
		}

		*r, err = collection.CountDocuments(ctx, cmd.Filter, opts)
		return err
	})
}

type DeleteOneCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Opts       *options.DeleteOneOptions
	OptsList   options.Lister[options.DeleteOneOptions]
}

// DeleteOne executes a delete command to delete at most one document from the collection.
func (m *MongoProxy) DeleteOne(payload []byte, result *[]byte) error {
	cmd := &DeleteOneCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.DeleteOne()

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		*r, err = collection.DeleteOne(ctx, cmd.Filter, opts)
		return err
	})
}

type DeleteManyCmd struct {
	Database   string
	Collection string
	Filter     bson.Raw
	Opts       *options.DeleteManyOptions
	OptsList   options.Lister[options.DeleteManyOptions]
}

// DeleteMany executes a delete command to delete documents from the collection.
func (m *MongoProxy) DeleteMany(payload []byte, result *[]byte) error {
	cmd := &DeleteManyCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.DeleteMany()
		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		*r, err = collection.DeleteMany(ctx, cmd.Filter, opts)
		return err
	})
}

type AggregateCmd struct {
	Database   string
	Collection string
	Pipeline   mongo.Pipeline
	Opts       *options.AggregateOptions
	OptsList   options.Lister[options.AggregateOptions]
}

// Aggregate executes an aggregate command against the collection and returns all the resulting documents.
func (m *MongoProxy) Aggregate(payload []byte, result *[]byte) error {
	cmd := &AggregateCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		var rr []interface{}
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		//	Comment                  any
		//	Hint                     any
		//	Let                      any
		//	Custom                   bson.M
		opts := options.Aggregate()

		if cmd.Opts.AllowDiskUse != nil {
			opts.SetAllowDiskUse(*cmd.Opts.AllowDiskUse)
		}

		if cmd.Opts.BatchSize != nil {
			opts.SetBatchSize(*cmd.Opts.BatchSize)
		}

		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		if cmd.Opts.MaxAwaitTime != nil {
			opts.SetMaxAwaitTime(*cmd.Opts.MaxAwaitTime)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		if cmd.Opts.Custom != nil {
			opts.SetCustom(cmd.Opts.Custom)
		}

		cursor, err := collection.Aggregate(ctx, cmd.Pipeline, opts)
		if cursor != nil {
			defer cursor.Close(ctx)
			err = cursor.All(ctx, &rr)
		}
		*r = rr
		return err
	})
}

type BulkWriteCmd struct {
	Database   string
	Collection string
	Operations []map[string][]bson.Raw
	Opts       *options.BulkWriteOptions
	OptsList   options.Lister[options.BulkWriteOptions]
}

func (m *MongoProxy) BulkWrite(payload []byte, result *[]byte) error {
	cmd := &BulkWriteCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)
		models := parseModels(cmd.Operations)

		opts := options.BulkWrite()

		if cmd.Opts.BypassDocumentValidation != nil {
			opts.SetBypassDocumentValidation(*cmd.Opts.BypassDocumentValidation)
		}

		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Let != nil {
			opts.SetLet(cmd.Opts.Let)
		}

		if cmd.Opts.Ordered != nil {
			opts.SetOrdered(*cmd.Opts.Ordered)
		}

		*r, err = collection.BulkWrite(ctx, models, opts)
		return err
	})
}

type DistinctCmd struct {
	Database   string
	Collection string
	FieldName  string
	Filter     bson.Raw
	Opts       *options.DistinctOptions
	OptsList   options.Lister[options.DistinctOptions]
}

// Distinct executes a distinct command to find the unique values for a specified field in the collection.
func (m *MongoProxy) Distinct(payload []byte, result *[]byte) error {
	cmd := &DistinctCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)

		opts := options.Distinct()
		if cmd.Opts.Comment != nil {
			opts.SetComment(cmd.Opts.Comment)
		}

		if cmd.Opts.Hint != nil {
			opts.SetHint(cmd.Opts.Hint)
		}

		if cmd.Opts.Collation != nil {
			opts.SetCollation(cmd.Opts.Collation)
		}

		return collection.Distinct(ctx, cmd.FieldName, cmd.Filter, opts).Err()
	})
}

type CreateIndexCmd struct {
	Database   string
	Collection string
	IndexKeys  bson.Raw
	Opts       *options.IndexOptionsBuilder
	CreateOpts *options.CreateIndexesOptions
	OptsList   options.Lister[options.CreateIndexesOptions]
}

func (m *MongoProxy) CreateIndex(payload []byte, result *[]byte) error {
	cmd := &CreateIndexCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)
		model := mongo.IndexModel{
			Keys:    cmd.IndexKeys,
			Options: cmd.Opts,
		}

		opts := options.IndexOptions

		*r, err = collection.Indexes().CreateOne(ctx, model, opts)
		return err
	})
}

type CreateIndexesCmd struct {
	Database   string
	Collection string
	Models     []mongo.IndexModel
	Opts       *options.IndexOptions
	CreateOpts *options.CreateIndexesOptions
	OptsList   options.Lister[options.CreateIndexesOptions]
}

func (m *MongoProxy) CreateIndexes(payload []byte, result *[]byte) error {
	cmd := &CreateIndexesCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)
		*r, err = collection.Indexes().CreateMany(ctx, cmd.Models, cmd.OptsList)
		return err
	})
}

type DropIndexCmd struct {
	Database   string
	Collection string
	Name       string
	Opts       *options.DropIndexesOptions
	OptsList   options.Lister[options.DropIndexesOptions]
}

func (m *MongoProxy) DropIndex(payload []byte, result *[]byte) error {
	cmd := &DropIndexCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)
		return collection.Indexes().DropOne(ctx, cmd.Name, cmd.OptsList)
	})
}

type DropIndexesCmd struct {
	Database   string
	Collection string
	Opts       *options.DropIndexesOptions
	OptsList   options.Lister[options.DropIndexesOptions]
}

func (m *MongoProxy) DropIndexes(payload []byte, result *[]byte) error {
	cmd := &DropIndexesCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)
		return collection.Indexes().DropAll(ctx, cmd.OptsList)
	})
}

type ListIndexesCmd struct {
	Database   string
	Collection string
	Opts       *options.ListIndexesOptions
	OptsList   options.Lister[options.ListIndexesOptions]
}

func (m *MongoProxy) ListIndexes(payload []byte, result *[]byte) error {
	cmd := &ListIndexesCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		var rr []interface{}
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)
		cursor, err := collection.Indexes().List(ctx, cmd.OptsList)
		if cursor != nil {
			defer cursor.Close(ctx)
			err = cursor.All(ctx, &rr)
			*r = rr
		}
		return err
	})
}

type DropCmd struct {
	Database   string
	Collection string
}

// Drop drops the collection on the server. This method ignores "namespace not found" errors so it is safe to drop
// a collection that does not exist on the server.
func (m *MongoProxy) Drop(payload []byte, result *[]byte) error {
	cmd := &DropCmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		collection := m.client.Database(cmd.Database).Collection(cmd.Collection)
		return collection.Drop(ctx)
	})
}

type Cmd struct {
	Database string
	Command  bson.D
	Opts     *options.RunCmdOptions
	OptsList options.Lister[options.RunCmdOptions]
}

// RunCommand executes the given command against the database.
func (m *MongoProxy) RunCommand(payload []byte, result *[]byte) error {
	cmd := &Cmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		database := m.client.Database(cmd.Database)
		return database.RunCommand(ctx, cmd.Command, cmd.OptsList).Decode(r)
	})
}

// RunCommandCursor executes the given command against the database and parses the response as a slice. If the command
// being executed does not return a slice, the command will be executed on the server and an error
// will be returned because the server response cannot be parsed as a slice.
func (m *MongoProxy) RunCommandCursor(payload []byte, result *[]byte) error {
	cmd := &Cmd{}
	return m.exec(cmd, payload, result, func(ctx context.Context, r *interface{}) (err error) {
		var rr []interface{}
		database := m.client.Database(cmd.Database)
		cursor, err := database.RunCommandCursor(ctx, cmd.Command, cmd.OptsList)
		if cursor != nil {
			defer cursor.Close(ctx)
			err = cursor.All(ctx, &rr)
		}
		*r = rr
		return err
	})
}
