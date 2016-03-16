package main

import (
	"os"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func GenerateTestConfig() Config {
	wd, _ := os.Getwd()
	config := Config{
		Console:            "echo",
		Site:               wd,
		URI:                "",
		DefaultWorkerCount: runtime.NumCPU() - 1,
		Context:            nil,
	}
	return config
}

func TestDBQueue(t *testing.T) {
	// open database stub
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("An error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Generate config.
	config := GenerateTestConfig()

	// columns are prefixed with "o" since we used sqlstruct to generate them
	columns := []string{"item_id", "name"}

	// expect query to fetch order and user, match it with regexp
	mock.ExpectQuery("SELECT item_id, name FROM queue WHERE expire = \\?").
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows(columns).FromCSVString("1,test_queue"))

	mock.ExpectPrepare("UPDATE queue SET expire").
		ExpectExec().
		WithArgs(sqlmock.AnyArg(), 1, 0).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Execute the scanner.
	scanNewItems(db, config, true)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestDBQueueHandle(t *testing.T) {
	// open database stub
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("An error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Generate config and add handle queues.
	config := GenerateTestConfig()
	config.HandleQueues = []string{"test_queue_handle", "test_queue_handle2"}

	// columns are prefixed with "o" since we used sqlstruct to generate them
	columns := []string{"item_id", "name"}

	// expect query to fetch order and user, match it with regexp
	mock.ExpectQuery("SELECT item_id, name FROM queue WHERE expire = \\? AND name IN \\(\\?\\)").
		WithArgs(0, strings.Join(config.HandleQueues, ",")).
		WillReturnRows(sqlmock.NewRows(columns).FromCSVString("2,test_queue_handle"))

	mock.ExpectPrepare("UPDATE queue SET expire = \\? WHERE item_id = \\? AND expire = \\?").
		ExpectExec().
		WithArgs(sqlmock.AnyArg(), 2, 0).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Execute the scanner.
	scanNewItems(db, config, true)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestDBQueueSkip(t *testing.T) {
	// open database stub
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("An error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Generate config and skip queues.
	config := GenerateTestConfig()
	config.SkipQueues = []string{"test_queue_skip", "test_queue_skip2"}

	// columns are prefixed with "o" since we used sqlstruct to generate them
	columns := []string{"item_id", "name"}

	// expect query to fetch order and user, match it with regexp
	mock.ExpectQuery("SELECT item_id, name FROM queue WHERE expire = \\? AND name NOT IN \\(\\?\\)").
		WithArgs(0, strings.Join(config.SkipQueues, ",")).
		WillReturnRows(sqlmock.NewRows(columns).FromCSVString(""))

	// Execute the scanner.
	scanNewItems(db, config, true)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestDBConnectionInfoParser(t *testing.T) {
	// Generate config.
	config := GenerateTestConfig()

	// Test normal DB info.
	config.Console = "./test_scripts/db_info.sh"
	connectString, err := getDBConnectString(config)
	if err != nil {
		t.Errorf("Could not parse DB details: %s", err)
	}

	if connectString != "user:password@tcp(testhost:3306)/test" {
		t.Errorf("Database string was not in expected format")
	}

	// Test DB info with port.
	config.Console = "./test_scripts/db_info_port.sh"
	connectString, err = getDBConnectString(config)
	if err != nil {
		t.Errorf("Could not parse DB details with port: %s", err)
	}

	if connectString != "user:password@tcp(testhost:2610)/test" {
		t.Errorf("Database string with port was not in expected format")
	}

	// Test DB multiline info with port.
	config.Console = "./test_scripts/db_info_multiline.sh"
	connectString, err = getDBConnectString(config)
	if err != nil {
		t.Errorf("Could not parse DB details with newlines: %s", err)
	}

	if connectString != "user:password@tcp(testhost:3306)/test_with_multiline_return" {
		t.Errorf("Database string with newlines was not in expected format")
	}
}

func TestWorkers(t *testing.T) {
	queues := map[string]*Queue{}

	config := GenerateTestConfig()
	cpuDefaultWorkerCount := runtime.NumCPU() - 1
	startNewQueue("test_queue_count_1", queues, config)

	if workers, ok := queues["test_queue_count_1"]; ok {
		if cap(workers.Jobs) != cpuDefaultWorkerCount {
			t.Errorf("Worker amount for queue %s did not match expected.", "test_queue_count_1")
		}
	} else {
		t.Errorf("Did not start workers for queue %s", "test_queue_count_1")
	}

	config.DefaultWorkerCount = 2
	startNewQueue("test_queue_count_2", queues, config)

	if workers, ok := queues["test_queue_count_2"]; ok {
		if cap(workers.Jobs) != 2 {
			t.Errorf("Worker amount for queue %s did not match expected.", "test_queue_count_2")
		}
	} else {
		t.Errorf("Did not start workers for queue %s", "test_queue_count_2")
	}

	config.QueueWorkers = map[string]int{
		"test_queue_count_3": 4,
	}
	startNewQueue("test_queue_count_3", queues, config)

	if workers, ok := queues["test_queue_count_3"]; ok {
		if cap(workers.Jobs) != 4 {
			t.Errorf("Worker amount for queue %s did not match expected.", "test_queue_count_3")
		}
	} else {
		t.Errorf("Did not start workers for queue %s", "test_queue_count_3")
	}
}
