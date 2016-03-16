# DirectQueue Go [![Build Status](https://travis-ci.org/VDMi/DirectQueueGo.svg?branch=master)](https://travis-ci.org/VDMi/DirectQueueGo)
Direct Queue worker for Drupal 8 in Go

This Go file watches the Drupal Queue and processes items as soon as it notices them or there is space in the queue.

Drupal Console is required for getting the database connection and processing queue items.

The Drupal module direct_queue is required to process items.
