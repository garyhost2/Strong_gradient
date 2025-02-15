package middleware

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Company represents the company structure
type Company struct {
	Name    string `bson:"name" json:"name"`
	Address string `bson:"address" json:"address"`
	Treated bool   `bson:"treated" json:"treated"`
}

// BatchProcessor handles operations related to batch processing
type BatchProcessor struct {
	client     *mongo.Client
	collection *mongo.Collection
	batchSize  int
	workers    int
}

// NewBatchProcessor creates a new BatchProcessor
func NewBatchProcessor(uri, dbName, collName string, batchSize, numWorkers int) (*BatchProcessor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Configure MongoDB client with proper options
	clientOptions := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(5 * time.Second).
		SetServerSelectionTimeout(5 * time.Second).
		SetMaxPoolSize(uint64(numWorkers * 2))

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// Verify connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	collection := client.Database(dbName).Collection(collName)

	// Create index on name field for faster lookups
	_, err = collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "name", Value: 1}},
		Options: options.Index().
			SetUnique(true).
			SetBackground(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %v", err)
	}

	return &BatchProcessor{
		client:     client,
		collection: collection,
		batchSize:  batchSize,
		workers:    numWorkers,
	}, nil
}

// HealthCheck performs a health check on the MongoDB connection
func (bp *BatchProcessor) HealthCheck(ctx context.Context) error {
	return bp.client.Ping(ctx, readpref.Primary())
}

// ProcessBatch processes and stores a batch of companies
func (bp *BatchProcessor) ProcessBatch(ctx context.Context, companies []Company) (int, error) {
	if len(companies) == 0 {
		return 0, nil
	}

	var operations []mongo.WriteModel
	for _, company := range companies {
		operation := mongo.NewUpdateOneModel().
			SetFilter(bson.M{"name": company.Name}).
			SetUpdate(bson.M{"$set": bson.M{
				"name":    company.Name,
				"address": company.Address,
				"treated": company.Treated,
			}}).
			SetUpsert(true)
		
		operations = append(operations, operation)
	}

	// Configure bulk write options
	opts := options.BulkWrite().
		SetOrdered(false)

	// Execute bulk write
	result, err := bp.collection.BulkWrite(ctx, operations, opts)
	if err != nil {
		return 0, fmt.Errorf("failed to process batch: %v", err)
	}

	totalModified := int(result.ModifiedCount + result.UpsertedCount)
	log.Printf("Processed %d companies (Modified: %d, Upserted: %d)",
		totalModified, result.ModifiedCount, result.UpsertedCount)

	return totalModified, nil
}

// UpdateTreatedField updates the 'treated' field of a company by name
func (bp *BatchProcessor) UpdateTreatedField(ctx context.Context, companyName string) error {
	filter := bson.M{"name": companyName}
	update := bson.M{"$set": bson.M{"treated": true}}

	result, err := bp.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update treated field: %v", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("company not found: %s", companyName)
	}

	if result.ModifiedCount == 0 {
		return fmt.Errorf("company found but no update performed: %s", companyName)
	}

	log.Printf("Updated treated field for company: %s", companyName)
	return nil
}

// FetchAllCompanies retrieves all companies from the database
func (bp *BatchProcessor) FetchAllCompanies(ctx context.Context) ([]Company, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "name", Value: 1}})

	cursor, err := bp.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch companies: %v", err)
	}
	defer cursor.Close(ctx)

	var companies []Company
	if err = cursor.All(ctx, &companies); err != nil {
		return nil, fmt.Errorf("failed to decode companies: %v", err)
	}

	return companies, nil
}

// Close closes the MongoDB connection
func (bp *BatchProcessor) Close(ctx context.Context) error {
	return bp.client.Disconnect(ctx)
}