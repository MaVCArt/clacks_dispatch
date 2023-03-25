# clacks_dispatch

Extension library for `clacks` that implements a dispatch server model which works with master and slave servers.
This model is designed to deal with high-demand setups that have to process many requests, or setups that need to queue
heavy operations that don't want to operate on the server machine.

**The `Dispatch Server` implementation is an experimental feature** and is built on the dispatch model, where a
single user-facing server consumes commands, but does not itself execute them. Instead, it "dispatches" those commands
directly to a "worker", which is another server instance that could theoretically exist anywhere, even on a different
machine within the same network.

This allows for an architecture where a relatively weak machine can function as a dispatch server, while a series of
much more powerful machines can run the individual "worker servers", which do the heavy lifting that engages lots of CPU
or GPU.

While a task is being run, or a worker is engaged, a worker is considered "engaged" and will not be acquired for any
other task.

If all workers are engaged, the dispatch server will issue a timeout result and tell the user that no worker could be
engaged.
