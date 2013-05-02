`wterl` is an Erlang interface to the WiredTiger database, and is written to
support a Riak storage backend that uses WiredTiger.

Remaining work includes:

TODO:
* Find/fix any code marked "TODO:"
  * Why do we see {error, {eperm, _}} result on wterl:cursor_close/1 during
    fold_objects/4?
  * Why do we see {error, {eperm, _}} result on wterl:cursor_close/1?
  * Why do we see {error, {eperm, _}} result on wterl:cursor_next/1 during
    is_empty/1?
  * Why do we see {error, {eperm, _}} result on wterl:cursor_next_value/1
    during status/1?
  * Why do we see {error, {ebusy, _}} result on wterl:drop/2?
  * Determine a better way to estimate the number of sessions we should
    configure WT for at startup in riak_kv_wterl_backend:max_sessions/1.
* Provide a way to configure the cursor options, right now they are
  always "raw,overwrite".
* Add support for Riak/KV 2i indexes using the same design pattern
  as eLevelDB (in a future version consider alternate schema)
* If an operation using a shared cursor results in a non-normal error
  then it should be closed/discarded from the recycled pool
* Cache cursors based on hash(table/config) rather than just table.
* Finish NIF unload/reload functions and test.
* Test an upgrade, include a format/schema/WT change.
* When WT_PANIC is returned first try to unload/reload then driver
  and reset all state, if that fails then exit gracefully.
* Currently the `riak_kv_wterl_backend` module is stored in this
  repository, but it really belongs in the `riak_kv` repository.
* wterl:truncate/5 can segv, and its tests are commented out
* Add async_nif and wterl NIF stats to the results provided by the
  stats API
* Longer term ideas/changes to consider:
  * More testing, especially pulse/qc
  * Riak/KV integration
    * Store 2i indexes in separate tables
    * Store buckets, in separate tables and keep a <<bucket/key>> index
      to ensure that folds across a vnode are easy
    * Provide a drop bucket API call
    * Support key expirey
    * An ets API (like the LevelDB's lets project)
    * Use mime-type to inform WT's schema for key value encoding
 * Other use cases within Riak
    * An AAE driver using WT
    * An ability to store the ring file via WT


Deploying
---------

You can deploy `wterl` into a Riak devrel cluster using the `enable-wterl`
script. Clone the `riak` repo, change your working directory to it, and
then execute the `enable-wterl` script. It adds `wterl` as a dependency,
runs `make all devrel`, and then modifies the configuration settings of the
resulting dev nodes to use the WiredTiger storage backend.
