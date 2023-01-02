package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/olivere/elastic/v7"
)

const (
	ConfigFilenameTemplate = "mapping_%s.json"
	ElasticsearchHost      = "http://127.0.0.1:9200"
)

var Indexes = []string{
	"books",
}

func getElasticClient() (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetURL(ElasticsearchHost), elastic.SetSniff(false))
	if err != nil {
		return nil, err
	}

	return client, nil
}

func elasticInit() error {
	// Create a new Elasticsearch client
	es, err := getElasticClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	for _, index := range Indexes {
		// Check if the index exists
		exists, err := es.IndexExists(index).Do(context.Background())
		if err != nil {
			return err
		}

		if exists {
			fmt.Printf("Index [%s]: STARTED\n", index)
		} else {
			fmt.Printf("Initializing Index [%s] with mapping...", index)
			mapping, err := loadMappingFile(index)
			if err != nil {
				return err
			}

			// Create the index with the mapping
			createIndex, err := es.CreateIndex(index).Body(string(mapping)).Do(context.Background())
			if err != nil {
				log.Fatalf("Error creating the index: %s", err)
			}

			// Check the response
			if !createIndex.Acknowledged {
				log.Fatalf("Error creating the index: %s", createIndex.Index)
			} else {
				fmt.Printf("Index created successfully\n")
			}

		}
	}

	return nil
}

func loadMappingFile(key string) (raw []byte, err error) {
	entityMappingFile := fmt.Sprintf(ConfigFilenameTemplate, key)
	if fileExists(entityMappingFile) {
		raw, err = ioutil.ReadFile(entityMappingFile)
		if err != nil {
			return nil, err
		}
	}
	return raw, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func getIndexMapping(index string) {
	// Create a new Elasticsearch client
	es, err := getElasticClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// Get the mapping for the index
	res, err := es.GetMapping().Index(index).Do(context.Background())
	if err != nil {
		log.Fatalf("Error getting the mapping: %s", err)
	}

	// Print the mapping
	mapping, _ := res[index].(map[string]interface{})
	fmt.Printf("Mapping for index '%s':\n%v\n", index, mapping)
}

func insertDoc(doc map[string]interface{}) {
	// Create a new Elasticsearch client
	es, err := getElasticClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	_, err = es.Index().
		Index(Indexes[0]).
		BodyJson(doc).
		Do(context.Background())
	if err != nil {
		log.Fatalf("Error inserting the document: %s", err)
	}

	fmt.Println("Document inserted successfully")
}

func searchElastic() {
	// Create a new Elasticsearch client
	es, err := getElasticClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// ** QUERIES ** //

	// Term Query -  simple query that searches for an exact match of the provided term in the field.
	//termQuery := elastic.NewTermQuery("author", "Clinton Gormley and Zachary Tong")

	// Match Query - full-text search that matches documents containing a specific string or phrase (flexible).
	//matchQuery := elastic.NewMatchQuery("author", "Zachary Tanga")

	// Should Clause - specifies that the matching documents should satisfy at least one of the conditions specified in the clause.
	shouldClause := elastic.NewBoolQuery().Should(elastic.NewMatchQuery("author", "Guilherme Gonçalves"), elastic.NewMatchQuery("author", "Zachary Tanga"))

	// Must Clause - specifies that the matching documents must satisfy all the conditions specified in the clause.
	//mustClause := elastic.NewBoolQuery().Must(elastic.NewMatchQuery("author", "Guilherme Gonçalves"), elastic.NewMatchQuery("author", "Zachary Tanga"))

	// Exists Clause - used to search for documents that have a specific field, or to filter out documents that do not have a specific field.
	//existsClause := elastic.NewExistsQuery("author")

	// Execute the search query
	res, err := es.Search().
		Index(Indexes...).
		Query(shouldClause).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	// Get the search results as JSON
	for _, hit := range res.Hits.Hits {
		// JSON parse or do whatever with each document retrieved from your index
		item := Book{}
		json.Unmarshal(hit.Source, &item)
		fmt.Println(item)
	}
}

func main() {
	elasticInit()
	//getIndexMapping(Indexes[0])
	//insertDoc(Doc1)
	searchElastic()
}
