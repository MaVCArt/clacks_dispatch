# clacks_dispatch

Extension library for `clacks` that implements a dispatch server model which works with master and slave servers.
This model is designed to deal with high-demand setups that have to process many requests, or setups that need to queue
heavy operations that don't want to operate on the server machine.

With the `dispatch` model, a user can set up a central "dispatch" server, which redirects incoming commands to a an 
available slave server. While a slave is engaged, it cannot be acquired again until its last queued question is 
complete and has returned a result to the dispatch server. This ensures that a slave server is never engaged by more
than one parent server.

## Example Setup

One possible use case for this setup is a master dispatch server that aggregates information that is continuously built
by all of its slaves. This build process could be very heavy, which would slow down the server's response time.

To keep this response time up, the dispatch server simply caches the built data, without building it itself. External
clients can then request this data very quickly, without need to wait for the data in question to complete building.
