package main

import (
	"db_indexing/hash_indexes"
	"fmt"
	"log"
)

func main() {
	hIndex, err := hash_indexes.NewHashIndex()
	if err != nil {
		log.Fatalf("Failed to create hash index: %v", err)
	}
	defer hIndex.Close()

	// Example usage
	key := []byte("exampleKey")
	value := []byte("exampleValue")

	err = hIndex.Put(key, value)
	if err != nil {
		log.Fatalf("Failed to put value: %v", err)
	}

	retrievedValue, err := hIndex.Get(key)
	if err != nil {
		log.Fatalf("Failed to get value: %v", err)
	}

	fmt.Printf("Retrieved value: %s\n", retrievedValue)

	key2 := []byte("exampleKey")
	value2 := []byte("Somethinglikethat")

	err = hIndex.Put(key2, value2)
	if err != nil {
		log.Fatalf("Failed to put value: %v", err)
	}

	retrievedValue2, err := hIndex.Get(key2)
	if err != nil {
		log.Fatalf("Failed to get value: %v", err)
	}

	fmt.Printf("Retrieved value: %s\n", retrievedValue2)
}
