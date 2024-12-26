package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jsmithdenverdev/poc-parquet-publisher/internal/models"
	"github.com/parquet-go/parquet-go"
)

const (
	targetSizeBytes = 1024 * 1024 * 1024 // 1GB
	bodyLength      = 1000               // length of random text in each record
)

var (
	firstNames      = []string{"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"}
	lastNames       = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	cities          = []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"}
	states          = []string{"NY", "CA", "IL", "TX", "AZ", "PA", "FL", "OH", "GA", "NC"}
	streets         = []string{"Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Washington St", "Park Ave", "Lake Dr", "River Rd"}
	countries       = []string{"USA", "Canada", "UK", "Australia", "Germany", "France", "Japan", "Brazil"}
	languages       = []string{"en", "es", "fr", "de", "it", "pt", "ja", "zh"}
	accountTypes    = []string{"free", "basic", "premium", "enterprise"}
	accountStatuses = []string{"active", "suspended", "pending", "closed"}
	commPrefs       = []string{"email", "sms", "phone", "mail"}
	tags            = []string{"vip", "new", "returning", "priority", "special_offer", "seasonal", "promotional"}
)

func main() {
	if err := run(context.Background(), os.Stdout, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stdout io.Writer, getenv func(string) string) error {
	outputFile := "test_data.parquet"
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	f, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer f.Close()

	// Create schema for our Record type
	writer := parquet.NewWriter(f, parquet.SchemaOf(new(models.Record)))
	defer writer.Close()

	const rowsPerFlush = 10000
	totalRows := 0

	for {
		// Write a batch of rows
		for i := 0; i < rowsPerFlush; i++ {
			err := writer.Write(generateRecord())
			if err != nil {
				return fmt.Errorf("writing record: %w", err)
			}
		}

		totalRows += rowsPerFlush

		// Check file size after writing a batch
		if totalRows%rowsPerFlush == 0 {
			if err := writer.Flush(); err != nil {
				panic(err)
			}

			fileInfo, err := f.Stat()
			if err != nil {
				return fmt.Errorf("getting file stats: %w", err)
			}

			fmt.Fprintf(stdout, "\rGenerated %.2f GB", float64(fileInfo.Size())/(1024*1024*1024))

			if fileInfo.Size() >= 1<<30 { // 1GB
				break
			}
		}
	}

	fmt.Fprintln(stdout, "\nDone!")
	return nil
}

func generateRecord() models.Record {
	now := time.Now()
	r := models.Record{
		ID:        uuid.New().String(),
		CreatedAt: now.Add(-time.Duration(rand.Intn(365)) * 24 * time.Hour),
		UpdatedAt: now,

		FirstName:   randomFromSlice(firstNames),
		LastName:    randomFromSlice(lastNames),
		Email:       generateEmail(),
		PhoneNumber: generatePhoneNumber(),
		DateOfBirth: generateDateOfBirth(),

		AccountType:    randomFromSlice(accountTypes),
		AccountStatus:  randomFromSlice(accountStatuses),
		LastLoginDate:  now.Add(-time.Duration(rand.Intn(30)) * 24 * time.Hour),
		AccountBalance: float64(rand.Intn(10000)) + rand.Float64(),

		Language:             randomFromSlice(languages),
		NewsletterSubscribed: rand.Float32() > 0.5,
		Body:                 generateRandomText(bodyLength),
	}

	// Set address
	r.Address.Street = fmt.Sprintf("%d %s", rand.Intn(9999), randomFromSlice(streets))
	r.Address.City = randomFromSlice(cities)
	r.Address.State = randomFromSlice(states)
	r.Address.PostalCode = fmt.Sprintf("%05d", rand.Intn(99999))
	r.Address.Country = randomFromSlice(countries)

	// Generate random communication preferences
	numPrefs := rand.Intn(len(commPrefs)) + 1
	r.CommunicationPreferences = make([]string, numPrefs)
	for i := 0; i < numPrefs; i++ {
		r.CommunicationPreferences[i] = randomFromSlice(commPrefs)
	}

	// Generate random tags
	numTags := rand.Intn(4)
	r.Tags = make([]string, numTags)
	for i := 0; i < numTags; i++ {
		r.Tags[i] = randomFromSlice(tags)
	}

	return r
}

func randomFromSlice(slice []string) string {
	return slice[rand.Intn(len(slice))]
}

func generateEmail() string {
	domains := []string{"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"}
	return fmt.Sprintf("%s.%s@%s",
		strings.ToLower(randomFromSlice(firstNames)),
		strings.ToLower(randomFromSlice(lastNames)),
		randomFromSlice(domains))
}

func generatePhoneNumber() string {
	return fmt.Sprintf("+1-%03d-%03d-%04d",
		rand.Intn(800)+200,
		rand.Intn(900)+100,
		rand.Intn(9000)+1000)
}

func generateDateOfBirth() string {
	year := rand.Intn(50) + 1950
	month := rand.Intn(12) + 1
	day := rand.Intn(28) + 1 // Using 28 to avoid invalid dates
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func generateRandomText(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
