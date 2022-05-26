package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		log.Fatal("MONGODB_URI missing from environnment, add it file .env")
	}

	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().
		ApplyURI(uri).
		SetServerAPIOptions(serverAPIOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	switch os.Args[1] {
	case "1":
		findOne(ctx, client)
	case "2":
		find(ctx, client)
	case "3":
		inc(ctx, client)
	case "4":
		distinct(ctx, client)
	default:
		log.Fatal("Invalid argument")
	}
}

func findOne(ctx context.Context, client *mongo.Client) {
	coll := client.Database("sample_mflix").Collection("movies")
	title := "Back to the Future"

	var result bson.M
	err := coll.FindOne(ctx, bson.D{{"title", title}}).Decode(&result)
	if err == mongo.ErrNoDocuments {
		log.Printf("No document found with title %s", title)
	}
	if err != nil {
		log.Fatal(err)
	}

	jsonData, err := json.MarshalIndent(result, "", "    ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s", jsonData)
}

func find(ctx context.Context, client *mongo.Client) {
	coll := client.Database("sample_training").Collection("zips")
	filter := bson.D{{"pop", bson.D{{"$lte", 50}}}}

	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}

	printCursor(ctx, cursor)
}

func printCursor(ctx context.Context, cursor *mongo.Cursor) {
	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatal(err)
	}

	for _, result := range results {
		printResult(result)
	}
}

func printResult(result bson.M) {
	output, err := json.MarshalIndent(result, "", "      ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", output)
}

func inc(ctx context.Context, client *mongo.Client) {
	coll := client.Database("sample_training").Collection("zips")
	updateResult, err := coll.UpdateOne(ctx,
		bson.D{{"city", "LOST SPRINGS"}},
		bson.D{{"$inc", bson.D{{"pop", 1}}}})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Matched %v documents and updated %v documents.\n",
		updateResult.MatchedCount, updateResult.ModifiedCount)

	var result bson.M
	err = coll.FindOne(ctx, bson.D{{"city", "LOST SPRINGS"}}).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}

	printResult(result)
}

func distinct(ctx context.Context, client *mongo.Client) {
	coll := client.Database("sample_mflix").Collection("movies")
	filter := bson.D{{"directors", "Natalie Portman"}}
	results, err := coll.Distinct(ctx, "title", filter)
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		fmt.Println(result)
	}
}
