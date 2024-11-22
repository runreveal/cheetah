package redis

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedis(t *testing.T) {
	// Initialize the Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis server address
		Password: "",               // No password set
		DB:       0,                // Use default DB
	})

	x := []byte(`{"name":"John Doe","age":30}`)

	ctx := context.Background()

	// Push a message onto the queue
	err := rdb.LPush(ctx, "myqueue", x).Err()
	if err != nil {
		log.Fatalf("Could not push message to queue: %v", err)
	}

	// Simulate a delay
	time.Sleep(200 * time.Millisecond)

	// Pop a message from the queue
	msg, err := rdb.BRPopLPush(ctx, "myqueue", "processing", 0).Bytes()
	defer func() {
		_, err := rdb.LRem(ctx, "processing", 1, msg).Result()
		if err != nil {
			log.Fatalf("Could not remove message from processing Queue: %v", err)
		} else {
			fmt.Printf("Removed message from processing Queue: %s\n", string(msg))
		}
	}()

	if err == redis.Nil {
		fmt.Println("Queue is empty")
	} else if err != nil {
		log.Fatalf("Could not pop message from queue: %v", err)
	} else {
		fmt.Printf("Popped message: %s\n", string(msg))
	}
	// Test code here
}
