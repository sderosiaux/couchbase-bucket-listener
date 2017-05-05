# Couchbase Bucket Listener [![Build Status](https://travis-ci.org/chtefi/couchbase-bucket-listener.svg?branch=master)](https://travis-ci.org/chtefi/couchbase-bucket-listener)

A tiny admin displaying some info in real-time about a particular bucket in a Couchbase cluster.

It's possible to listen to any server / any bucket thanks to a smart URL pattern:
`http://localhost:8080/ui/$host/$bucket`

Example: `http://localhost:8080/ui/couchbase01/travel`

You'll get this UI updated in real-time:

![ui](ui.gif)

# Features

It displays:
- The total of events starting at the time you connect.
- A chart per events containing the delta per interval.
- The N last mutated documents of the bucket, with their expiry if any.

It is refreshed every 200ms by default, but this is configurable:
- `http://localhost:8080/ui/couchbase01/travel?interval=1000`: refresh every second
- `http://localhost:8080/ui/couchbase01/travel?interval=1000&n=100`: displays the 100 last mutated documents
 
It's also possible to click on those last mutated documents keys (displayed at the bottom), to display its content on the right.

- If the bucket is password protected, just set the query parameter `pwd`: `&pwd=xxx`.
- It's possible to filter only on certain keys using `filter`: `&filter=102:` to grab keys containing matching `*102:*`.

## Parameters list

All are optional:

- interval=1000 (milliseconds)
- n=100 (list of mutated document keys)
- password=xxx (for password protected buckets)
- filter=xxx (to filter only on certain keys)

## Bucket lists

It's possible to list all the available buckets, with hyperlinks to navigate quickly:

`http://localhost:8080/ui/couchbase01?user=admin&pwd=admin`

# Internals

This project is using:

- Couchbase Java DCP Client
- Couchbase Java Client
- Akka Streams to stream the Couchbase events properly in the backend with backpressure please 
- Akka-HTTP
- Akka-SSE because websockets are overrated
- Smoothie charts for the smooth charts

# Notes

DCP exposes `Expiration` messages that are taken into account here but are not displayed.
Couchbase 4.5.0 does not even emit them yet, so it's just hidden in the UI until further notice.
