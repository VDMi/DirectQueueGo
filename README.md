# DirectQueue Go [![Build Status](https://travis-ci.org/VDMi/DirectQueueGo.svg?branch=master)](https://travis-ci.org/VDMi/DirectQueueGo)
Direct Queue worker for Drupal 8 in Go

This Go file watches the Drupal Queue and processes items as soon as it notices them or there is space in the queue.

Drupal Console is required for getting the database connection and processing queue items.

The Drupal module [direct_queue](https://www.drupal.org/project/direct_queue) is required to process items.

## Running
Download any of the releases that matches your platform. You don't need any dependencies to run the program (besides being able to run Drupal Console). The workers can also run on a separate machine as long as the Drupal codebase, Drupal Console and database connection are available.

You can run the program by executing it on the shell:
```
./linux_amd64_DirectQueueGo --site "/var/www/mysite.com" --uri "http://mysite.com"
```
This will start the program with the default settings. See ```./linux_amd64_DirectQueueGo help``` for the available configuration.

You can also wrap it in a upstart/init.d script, examples on that will come soon.

## Options
| Parameter              | Default Value    | Description  |
| ---------------------- | --------------   | ------------ |
| --console              | "console"        | Path to Drupal Console. Full path or just binary to search in $PATH. |
| --site                 | ""               | Full path to the Drupal root. |
| --uri                  | ""               | URI to pass to Drupal, useful when you are generating links. |
| --skip-queues          | ""               | Comma separated list of queues to skip, can't be used together with handle-queues. |
| --handle-queues        | ""               | Comma separated list of queues to handle, can't be used together with skip-queues. |
| --queue-workers        | ""               | Amount of workers to use per queue, format: "publish_scheduler:1,entity_update:4". |
| --default-worker-count | Amount of CPUs-1 | Default amount of workers, default value is amount of CPUs - 1. |
| --db-password          | DB Password      | Password for the database if it can't be read by Drupal Console. |

## Compiling
Make sure you are able to run a Go 1.6 (though 1.4/1.5 should also work) environment.
```
git clone https://github.com/VDMi/DirectQueueGo.git
cd DirectQueueGo
go install
go build
./DirectQueueGo help
```

After the ```go build```, ```DirectQueueGo``` will contain the compiled binary.
You can also use ```go run``` to run the program without compiling.



