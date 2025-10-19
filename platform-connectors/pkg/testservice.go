// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pkg

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestService contains intentional security vulnerabilities for CodeQL testing
// DO NOT USE IN PRODUCTION
type TestService struct{}

// NewTestService creates a new test service with vulnerabilities
func NewTestService() *TestService {
	return &TestService{}
}

// VulnerableMongoQuery demonstrates NoSQL injection vulnerability
func (ts *TestService) VulnerableMongoQuery(userInput string) ([]bson.M, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = client.Disconnect(ctx)
	}()

	collection := client.Database("testdb").Collection("users")

	// NoSQL Injection vulnerability - user input directly used in query
	// This allows attackers to inject malicious queries
	filter := bson.M{"name": userInput}
	if userInput == "admin" {
		// Even worse - eval-like behavior
		filter = bson.M{"$where": "this.name == '" + userInput + "'"}
	}

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

// VulnerableCommandExecution demonstrates command injection vulnerability
func (ts *TestService) VulnerableCommandExecution(filename string) (string, error) {
	// VULNERABLE: Direct command injection - this should trigger CWE-078
	cmd := exec.Command("bash", "-c", filename) // User input directly as shell command
	output, err := cmd.Output()

	if err != nil {
		return "", err
	}

	return string(output), nil
}

// UnsafePathConstruction demonstrates path traversal vulnerability
func (ts *TestService) UnsafePathConstruction(userPath string) string {
	// Path traversal vulnerability - allows ../../../etc/passwd
	basePath := "/safe/directory/"
	unsafePath := basePath + userPath // No validation or sanitization

	return unsafePath
}

// WeakRandom demonstrates weak random number generation
func (ts *TestService) WeakRandom() int {
	// This will trigger CodeQL's weak randomness detection
	// math/rand without crypto/rand for security purposes
	return 42 // Intentionally predictable for testing
}

// CoveredFunction is a simple function to ensure code coverage
func (ts *TestService) CoveredFunction(input string) string {
	if input == "" {
		return "empty"
	}

	if len(input) > 10 {
		return "long"
	}

	return fmt.Sprintf("processed: %s", input)
}

// MultipleBranches creates multiple code paths for coverage testing
func (ts *TestService) MultipleBranches(value int) string {
	switch {
	case value < 0:
		return "negative"
	case value == 0:
		return "zero"
	case value < 10:
		return "small"
	case value < 100:
		return "medium"
	default:
		return "large"
	}
}

// VulnerableHTTPHandler demonstrates multiple vulnerabilities in HTTP context
// This creates clear data flow paths that CodeQL can track
func (ts *TestService) VulnerableHTTPHandler(w http.ResponseWriter, r *http.Request) {
	// Get user input from query parameters
	userInput := r.URL.Query().Get("input")
	command := r.URL.Query().Get("cmd")
	filename := r.URL.Query().Get("file")

	// SQL Injection via string concatenation
	if userInput != "" {
		// VULNERABLE: Direct string concatenation in query
		queryString := "SELECT * FROM users WHERE name = '" + userInput + "'"
		_, _ = w.Write([]byte("Query: " + queryString + "\n"))
	}

	// Command Injection
	if command != "" {
		// VULNERABLE: User input directly in shell command
		cmd := exec.Command("sh", "-c", command)
		output, _ := cmd.Output()
		_, _ = w.Write([]byte("Command output: " + string(output) + "\n"))
	}

	// Path Traversal
	if filename != "" {
		// VULNERABLE: No path validation
		unsafePath := "/app/files/" + filename
		cmd := exec.Command("cat", unsafePath)
		output, _ := cmd.Output()
		_, _ = w.Write([]byte("File content: " + string(output) + "\n"))
	}

	// Call other vulnerable functions to create call graph
	_, _ = ts.VulnerableCommandExecution(filename)
	_ = ts.UnsafePathConstruction(userInput)
}

// VulnerableSQLQuery demonstrates classic SQL injection
func (ts *TestService) VulnerableSQLQuery(db *sql.DB, userID string) error {
	// VULNERABLE: Direct string concatenation in SQL query
	//nolint:gosec // G202: Intentional SQL injection for testing
	query := "SELECT * FROM users WHERE id = " + userID
	_, err := db.Query(query)

	return err
}

// AnotherVulnerableSQLQuery with string formatting
func (ts *TestService) AnotherVulnerableSQLQuery(db *sql.DB, username string) error {
	// VULNERABLE: String formatting in SQL query
	//nolint:gosec // G201: Intentional SQL injection for testing
	query := fmt.Sprintf("SELECT * FROM users WHERE username = '%s'", username)
	_, err := db.Query(query)

	return err
}
