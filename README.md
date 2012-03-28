`wterl` is an Erlang interface to the WiredTiger database, and is written
to support a Riak storage backend that uses WiredTiger.

This backend currently supports only key-value storage and retrieval.

Remaining work includes:

* The `wterl:session_create` function currently returns an error under
  certain circumstances, so we currently ignore its return value.
* The `riak_kv_wterl_backend` module is currently designed to rely on the
  fact that it runs in just a single Erlang scheduler thread, which is
  necessary because WiredTiger doesn't allow a session to be used
  concurrently by different threads. If the KV node design ever changes to
  involve concurrency across scheduler threads, this current design will no
  longer work correctly.
* Currently the `riak_kv_wterl_backend` module is stored in this
  repository, but it really belongs in the `riak_kv` repository.
* There are currently some stability issues with WiredTiger that can
  sometimes cause errors when restarting KV nodes with non-empty WiredTiger
  storage.

Future support for secondary indexes requires WiredTiger features that are
under development but are not yet available.

Deploying
---------

You can deploy `wterl` into a Riak devrel cluster using the `enable-wterl`
script. Clone the `riak` repo, change your working directory to it, and
then execute the `enable-wterl` script. It adds `wterl` as a dependency,
runs `make all devrel`, and then modifies the configuration settings of the
resulting dev nodes to use the WiredTiger storage backend.
