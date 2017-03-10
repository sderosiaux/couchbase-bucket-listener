# Couchbase Listener

A tiny admin displaying some info in real-time about a particular bucket in a Couchbase cluster.

It's possible to listen to any server / any bucket thanks to a smart URL pattern:
`http://localhost:8080/ui/$host/$bucket`

Example: `http://localhost:8080/ui/couchbase01/travel`

You'll get this UI updated in real-time:

![ui](ui.png)

# Features

It displays:
- The total of events starting at the time you connect.
- A chart per events containing the delta per interval.
- The N last mutated documents of the bucket, with their expiry if any.

It is refreshed every 200ms by default, but this is configurable:
- `http://localhost:8080/ui/couchbase01/travel?interval=1000`: refresh every second
- `http://localhost:8080/ui/couchbase01/travel?interval=1000&n=100`: displays the 100 last mutated documents
 
It's also possible to click on those last mutated documents keys (displayed at the bottom), to display its content on the right.

# Internals

This project is using:

- Couchbase Java DCP Client
- Couchbase Java Client
- Akka Streams to stream the Couchbase events properly in the backend with backpressure please 
- Akka-HTTP
- Akka-SSE because websockets are overrated
- Smoothie charts for the smooth charts
