package main

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	_ "github.com/go-sql-driver/mysql"
)

type QueueItem struct {
	ItemID uint64
	Expire uint64
}

type Queue struct {
	Name string
	Jobs chan QueueItem
}

type Config struct {
	Console            string
	Site               string
	URI                string
	Password           string
	Port               string
	HandleQueues       []string
	SkipQueues         []string
	QueueWorkers       map[string]int
	DefaultWorkerCount int
	Context            *cli.Context
}

// Default version. Travis will overwrite this.
var Version = "build-from-source"

func main() {
	app := cli.NewApp()

	app.Version = Version

	var (
		console            string
		drush              string
		site               string
		uri                string
		skipQueues         string
		handleQueues       string
		queueWorkers       string
		defaultWorkerCount int
		password           string
		port               string
	)

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "console",
			Value:       "drush",
			Usage:       "Binary of Drush. Full path or just binary to search in $PATH. (either use console or drush, console only exists for backwards compatibility)",
			Destination: &console,
		},
		cli.StringFlag{
			Name:        "drush",
			Value:       "drush",
			Usage:       "Binary of Drush. Full path or just binary to search in $PATH.",
			Destination: &drush,
		},
		cli.StringFlag{
			Name:        "site",
			Value:       "",
			Usage:       "Full path to the Drupal root.",
			Destination: &site,
		},
		cli.StringFlag{
			Name:        "uri",
			Value:       "",
			Usage:       "URI to pass to Drupal Console, for multi-site environments or if you format urls to your site inside the queues.",
			Destination: &uri,
		},
		cli.StringFlag{
			Name:        "skip-queues",
			Value:       "",
			Usage:       "Comma separated list of queues to skip, can't be used together with handle-queues.",
			Destination: &skipQueues,
		},
		cli.StringFlag{
			Name:        "handle-queues",
			Value:       "",
			Usage:       "Comma separated list of queues to handle, can't be used together with skip-queues.",
			Destination: &handleQueues,
		},
		cli.StringFlag{
			Name:        "queue-workers",
			Value:       "",
			Usage:       "Amount of workers to use per queue, format: \"publish_queue:1,screenshot_queue:4\". Default is amount of CPUs - 1.",
			Destination: &queueWorkers,
		},
		cli.IntFlag{
			Name:        "default-worker-count",
			Value:       runtime.NumCPU() - 1,
			Usage:       "Default amount of workers, default value is amount of CPUs - 1.",
			Destination: &defaultWorkerCount,
		},
		cli.StringFlag{
			Name:        "db-password",
			Value:       "",
			Usage:       "Overwrite the db password.",
			Destination: &password,
		},
		cli.StringFlag{
			Name:        "db-port",
			Value:       "3306",
			Usage:       "Set the database port.",
			Destination: &port,
		},
	}
	app.Name = "DirectQueue"
	app.Usage = "Directly handles Queue items by using a Go daemon."

	app.Action = func(c *cli.Context) {
		if drush != "" {
			console = drush
		}
		config := Config{
			Console:            console,
			Site:               site,
			URI:                uri,
			Password:           password,
			Port:               port,
			DefaultWorkerCount: defaultWorkerCount,
			Context:            c,
		}
		// Make sure we have the required variables.
		if site == "" {
			log.Fatal("Site path can't be empty.")
			os.Exit(1)
		}
		if console == "" {
			log.Fatal("Console path can't be empty.")
			os.Exit(1)
		}

		// We can't skip queues and have the joys of explicit queues.
		if skipQueues != "" && handleQueues != "" {
			log.Fatal("Can't use skip-queues and handle-queues together.")
			os.Exit(1)
		}

		// Parse skip queues variable from comma-separated to array.
		if skipQueues != "" {
			for _, value := range strings.Split(skipQueues, ",") {
				config.SkipQueues = append(config.SkipQueues, strings.TrimSpace(value))
			}
		}

		// Parse handle queues variable from comma-separated to array.
		if handleQueues != "" {
			for _, value := range strings.Split(handleQueues, ",") {
				config.HandleQueues = append(config.HandleQueues, strings.TrimSpace(value))
			}
		}

		// Parse queue workers variable from comma-separated to array.
		if queueWorkers != "" {
			config.QueueWorkers = map[string]int{}
			for _, value := range strings.Split(queueWorkers, ",") {

				// Split by semi-colon.
				// First part is queue.
				worker_parts := strings.Split(value, ":")
				if len(worker_parts) < 2 {
					log.Fatal("Wrong format for queue-workers.")
					os.Exit(1)
				}

				// Second part is amount of workers, should be int.
				worker_amount, err := strconv.ParseInt(worker_parts[1], 10, 32)
				if err != nil {
					log.Fatal("Wrong format for queue-workers.")
					os.Exit(1)
				}

				config.QueueWorkers[worker_parts[0]] = int(worker_amount)
			}
		}

		// Get the DB connection.
		db_connect, err := getDBConnectString(config)
		if err != nil {
			log.Fatal("Could not get DB details.", err)
			os.Exit(1)
		}

		// Open the DB connection.
		db, err := sql.Open("mysql", db_connect)
		if err != nil {
			log.Fatal("Could not connect to database.", err)
			os.Exit(1)
		}

		// Start the infinte scan for new queue items.
		scanNewItems(db, config, false)
	}

	app.Run(os.Args)
}

// Function to scan for queue items infinitely.
func scanNewItems(db *sql.DB, config Config, testmode bool) {
	log.Println("Scanning queue for new items...")

	queues := map[string]*Queue{}
	for {
		var (
			err  error
			rows *sql.Rows
		)

		// Make sure we use the right query for the queues we handle.
		if len(config.HandleQueues) > 0 {
			rows, err = db.Query("SELECT item_id, name FROM queue WHERE expire = ? AND name IN (?)", 0, strings.Join(config.HandleQueues, ","))
		} else if len(config.SkipQueues) > 0 {
			rows, err = db.Query("SELECT item_id, name FROM queue WHERE expire = ? AND name NOT IN (?)", 0, strings.Join(config.SkipQueues, ","))
		} else {
			rows, err = db.Query("SELECT item_id, name FROM queue WHERE expire = ?", 0)
		}

		if err != nil {
			log.Fatal("Could not query for queue items", err)
			time.Sleep(time.Second * 5)
			continue
		}

		// Loop through all the rows.
		for rows.Next() {
			var (
				item_id uint64
				name    string
			)

			// Transform the DB row into our variables.
			err := rows.Scan(&item_id, &name)
			if err != nil {
				log.Fatal(err)
				continue
			}

			// Create a new queue if we don't have one yet.
			if queues[name] == nil {
				startNewQueue(name, queues, config)
			}

			// Handle the current queue item.
			handleItem(db, item_id, name, queues, config)
		}

		// Close the row handler and wait 500 ms before querying again.
		rows.Close()
		time.Sleep(time.Millisecond * 500)

		if testmode {
			break
		}
	}
}

// Function to handle a specific queue item.
func handleItem(db *sql.DB, item_id uint64, queue_name string, queues map[string]*Queue, config Config) error {
	current_timestamp := uint64(time.Now().Unix())
	expire_timestamp := current_timestamp + 300

	// Claim the item for 5 minutes.
	// Make sure expire is still 0 so we won't claim twice.
	// Drupal cron will reset if expire has passed.
	stmt, err := db.Prepare("UPDATE queue SET expire = ? WHERE item_id = ? AND expire = ?")
	if err != nil {
		log.Fatal("Could not claim item", err)
		return err
	}
	res, err := stmt.Exec(expire_timestamp, item_id, 0)
	if err != nil {
		stmt.Close()
		log.Fatal("Could not claim item", err)
		return err
	}

	// Make sure the affected a row.
	// We need to make sure we actually claimed the item.
	rowCnt, err := res.RowsAffected()
	if err != nil {
		stmt.Close()
		log.Fatal("Could not claim item", err)
		return err
	}
	if rowCnt > 0 {

		// Queue item is claimed, add to channel.
		queues[queue_name].Jobs <- QueueItem{
			ItemID: item_id,
			Expire: expire_timestamp,
		}
	}

	// Close the update cursor.
	stmt.Close()

	return nil
}

// Function to start a new queue. Triggered when coming across a new queue.
func startNewQueue(name string, queues map[string]*Queue, config Config) {

	// Check how many worker we should create.
	worker_count := config.DefaultWorkerCount
	if val, ok := config.QueueWorkers[name]; ok {
		worker_count = val
	}

	log.Printf("Adding queue %s with %d workers", name, worker_count)

	// Create the new queue object.
	// The make() defines the buffer size (worker_count).
	newQueue := Queue{
		Name: name,
		Jobs: make(chan QueueItem, worker_count),
	}
	queues[name] = &newQueue

	// Start n workers per queue. Every worker is a subroutine.
	for i := 1; i <= worker_count; i++ {

		// Fix scoping of the working count.
		my_worker := i
		go func() {
			log.Printf("Starting worker %d for queue %s", my_worker, name)
			queueJobHandler(newQueue, config, my_worker)
		}()
	}
}

// Function to start a infinite loop to process job items.
func queueJobHandler(queue Queue, config Config, worker int) {
	// Infinite loop to process jobs.
	for {
		// Select waits until the queue unblocks.
		// This means that it will receive 1 item from the queue,
		// process it, and then wait for the next item in the queue.
		select {

		// Currently we only handle 1 channel.
		case job := <-queue.Jobs:

			// Execute the Drupal Console command.
			log.Printf("Started on queue %s, item %d, worker %d", queue.Name, job.ItemID, worker)
			_, err := executeCommand(config, []string{"direct_queue:run", strconv.FormatUint(job.ItemID, 10), strconv.FormatUint(job.Expire, 10)})
			if err != nil {
				log.Printf("Error on queue %s, item %d, worker %d", queue.Name, job.ItemID, worker)
				log.Fatal(err)
				continue
			}

			log.Printf("Finished on queue %s, item %d, worker %d", queue.Name, job.ItemID, worker)
			// @todo: do something with the output?
			// log.Println(string(bytes))
		}
	}
}

// Function to get the DB connection details from Drupal Console.
func getDBConnectString(config Config) (db_connect string, err error) {

	// Execute the database:connect command in Drupal Console.
	bytes, err := executeCommand(config, []string{"sql-connect"})
	if err != nil {
		return "", err
	}

	// Get output and replace -A with nothing
	output := strings.Replace(string(bytes), "-A", "", 1)

	// Try to parse database details from Drupal Console output.
	log.Printf("T" + output)
	re := regexp.MustCompile("(?ms)--user=(.*)--password=(.*)--database=(.*)--host=(.*)(?:--port=(0|[1-9][0-9]*|))?")
	matches := re.FindStringSubmatch(output)
	cutset := " \n\r"
	// See if we have enough matches.
	if len(matches) < 5 {
		err = errors.New("Could not find connection details.")
		return "", err
	}

	// Add default port of MySQL.
	if strings.Trim(matches[5], cutset) == "" {
		matches[5] = config.Port
	}

	if config.Password != "" {
		matches[2] = config.Password
	}

	// Put together the connection details.
	// matches[1] = --user
	// matches[2] = --password
	// matches[3] = --database
	// matches[4] = --host
	// matches[5] = --port (or default)

	db_connect = strings.Trim(matches[1], cutset) + ":" + strings.Trim(matches[2], cutset) + "@tcp(" + strings.Trim(matches[4], cutset) + ":" + strings.Trim(matches[5], cutset) + ")/" + strings.Trim(matches[3], cutset)

	return db_connect, nil
}

// Function to execute Drupal Console commands.
func executeCommand(config Config, args []string) ([]byte, error) {
	// We do not want colors.
	args = append(args, "--no-ansi")

	// Add Drupal Root.
	args = append(args, "--root="+config.Site)

	// Give the Drupal URI to Drupal Console when we need it.
	if config.URI != "" {
		args = append(args, "--uri="+config.URI)
	}

	cmd := exec.Command(config.Console, args...)
	// Make sure our site dir is the current working directory.
	if config.Site != "" {
		cmd.Dir = config.Site
	}
	return cmd.Output()
}
