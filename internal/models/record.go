package models

import "time"

// Record represents a data record with various fields
type Record struct {
	ID        string    `json:"id" parquet:"id"`
	CreatedAt time.Time `json:"created_at" parquet:"created_at"`
	UpdatedAt time.Time `json:"updated_at" parquet:"updated_at"`

	// Personal Information
	FirstName   string `json:"first_name" parquet:"first_name"`
	LastName    string `json:"last_name" parquet:"last_name"`
	Email       string `json:"email" parquet:"email"`
	PhoneNumber string `json:"phone_number" parquet:"phone_number"`
	DateOfBirth string `json:"date_of_birth" parquet:"date_of_birth"`

	// Address Information
	Address Address `json:"address" parquet:"address"`

	// Account Information
	AccountType    string    `json:"account_type" parquet:"account_type"`
	AccountStatus  string    `json:"account_status" parquet:"account_status"`
	LastLoginDate  time.Time `json:"last_login_date" parquet:"last_login_date"`
	AccountBalance float64   `json:"account_balance" parquet:"account_balance"`

	// Preferences
	Language                 string   `json:"language" parquet:"language"`
	CommunicationPreferences []string `json:"communication_preferences" parquet:"communication_preferences,list"`
	NewsletterSubscribed     bool     `json:"newsletter_subscribed" parquet:"newsletter_subscribed"`

	// Metadata
	Tags []string `json:"tags" parquet:"tags,list"`
	Body string   `json:"body" parquet:"body"`
}

// Address represents a physical address
type Address struct {
	Street     string `json:"street" parquet:"street"`
	City       string `json:"city" parquet:"city"`
	State      string `json:"state" parquet:"state"`
	PostalCode string `json:"postal_code" parquet:"postal_code"`
	Country    string `json:"country" parquet:"country"`
}
