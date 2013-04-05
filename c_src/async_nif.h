/*
 * async_nif: An async thread-pool layer for Erlang's NIF API
 *
 * Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 * Author: Gregory Burd <greg@basho.com> <greg@burd.me>
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef __ASYNC_NIF_H__
#define __ASYNC_NIF_H__

#if defined(__cplusplus)
extern "C" {
#endif

#include "queue.h"

#define ASYNC_NIF_MAX_WORKERS 32

struct async_nif_req_entry {
  ERL_NIF_TERM ref, *argv;
  ErlNifEnv *env;
  ErlNifPid pid;
  void *args;
  void (*fn_work)(ErlNifEnv*, ERL_NIF_TERM, ErlNifPid*, unsigned int, void *);
  void (*fn_post)(void *);
  STAILQ_ENTRY(async_nif_req_entry) entries;
};

struct async_nif_worker_entry {
  ErlNifTid tid;
  LIST_ENTRY(async_nif_worker_entry) entries;
};

struct async_nif_state {
  volatile unsigned int req_count;
  volatile unsigned int shutdown;
  ErlNifMutex *req_mutex;
  ErlNifMutex *worker_mutex;
  ErlNifCond *cnd;
  STAILQ_HEAD(reqs, async_nif_req_entry) reqs;
  LIST_HEAD(workers, async_nif_worker_entry) workers;
  unsigned int num_workers;
  struct async_nif_worker_entry worker_entries[ASYNC_NIF_MAX_WORKERS];
};

#define ASYNC_NIF_DECL(decl, frame, pre_block, work_block, post_block)  \
  struct decl ## _args frame;                                           \
  static void fn_work_ ## decl (ErlNifEnv *env, ERL_NIF_TERM ref, ErlNifPid *pid, unsigned int worker_id, struct decl ## _args *args) work_block \
  static void fn_post_ ## decl (struct decl ## _args *args) {           \
    do post_block while(0);						\
  }                                                                     \
  static ERL_NIF_TERM decl(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv_in[]) { \
    struct decl ## _args on_stack_args;                                 \
    struct decl ## _args *args = &on_stack_args;                        \
    struct decl ## _args *copy_of_args;                                 \
    struct async_nif_req_entry *req = NULL;                             \
    ErlNifEnv *new_env = NULL;                                          \
    /* argv[0] is a ref used for selective recv */                      \
    const ERL_NIF_TERM *argv = argv_in + 1;                             \
    argc--;                                                             \
    async_nif = (struct async_nif_state*)enif_priv_data(env);		\
    if (async_nif->shutdown)						\
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "shutdown"));         \
    if (!(new_env = enif_alloc_env())) {                                \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "enomem"));           \
    }                                                                   \
    do pre_block while(0);                                              \
    req = (struct async_nif_req_entry*)enif_alloc(sizeof(struct async_nif_req_entry)); \
    if (!req) {                                                         \
      fn_post_ ## decl (args);                                          \
      enif_free_env(new_env);                                           \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "enomem"));           \
    }                                                                   \
    copy_of_args = (struct decl ## _args *)enif_alloc(sizeof(struct decl ## _args)); \
    if (!copy_of_args) {                                                \
      fn_post_ ## decl (args);                                          \
      enif_free_env(new_env);                                           \
      return enif_make_tuple2(env, enif_make_atom(env, "error"),        \
                              enif_make_atom(env, "enomem"));           \
    }                                                                   \
    memcpy(copy_of_args, args, sizeof(struct decl ## _args));           \
    req->env = new_env;                                                 \
    req->ref = enif_make_copy(new_env, argv_in[0]);                     \
    enif_self(env, &req->pid);                                          \
    req->args = (void*)copy_of_args;                                    \
    req->fn_work = (void (*)(ErlNifEnv *, ERL_NIF_TERM, ErlNifPid*, unsigned int, void *))fn_work_ ## decl ; \
    req->fn_post = (void (*)(void *))fn_post_ ## decl;                  \
    return async_nif_enqueue_req(async_nif, req);			\
  }

#define ASYNC_NIF_LOAD() async_nif_load();
#define ASYNC_NIF_UNLOAD() async_nif_unload();
//define ASYNC_NIF_RELOAD()
#define ASYNC_NIF_UPGRADE() async_nif_unload();

#define ASYNC_NIF_RETURN_BADARG() return enif_make_badarg(env);
#define ASYNC_NIF_WORK_ENV new_env

#ifndef PULSE_FORCE_USING_PULSE_SEND_HERE
#define ASYNC_NIF_REPLY(msg) enif_send(NULL, pid, env, enif_make_tuple2(env, ref, msg))
#else
#define ASYNC_NIF_REPLY(msg) PULSE_SEND(NULL, pid, env, enif_make_tuple2(env, ref, msg))
#endif

static ERL_NIF_TERM
async_nif_enqueue_req(struct async_nif_state* async_nif, struct async_nif_req_entry *req)
{
  /* If we're shutting down return an error term and ignore the request. */
  if (async_nif->shutdown) {
    return enif_make_tuple2(req->env, enif_make_atom(req->env, "error"),
			    enif_make_atom(req->env, "shutdown")));
  }

  /* Otherwise, add the request to the work queue. */
  enif_mutex_lock(async_nif->req_mutex);
  STAILQ_INSERT_TAIL(&async_nif->reqs, req, entries);
  async_nif->req_count++;
  enif_mutex_unlock(async_nif->req_mutex);
  enif_cond_broadcast(async_nif->cnd);

  return enif_make_tuple2(env, enif_make_atom(env, "ok"),
                          enif_make_tuple2(env, enif_make_atom(env, "enqueued"),
					   enif_make_int(env, async_nif->req_count))); \
}

static void *async_nif_worker_fn(void *arg)
{
  struct async_nif_worker_entry *worker = (struct async_nif_worker_entry *)arg;
  struct async_nif_req_entry *req = NULL;

  /*
   * Workers are active while there is work on the queue to do and
   * only in the idle list when they are waiting on new work.
   */
  for(;;) {
    /* Examine the request queue, are there things to be done? */
    enif_mutex_lock(async_nif->req_mutex);
    enif_mutex_lock(async_nif->worker_mutex);
    LIST_INSERT_HEAD(&async_nif->workers, worker, entries);
    enif_mutex_unlock(async_nif->worker_mutex);
    check_again_for_work:
    if (async_nif->shutdown) { enif_mutex_unlock(async_nif->req_mutex); break; }
    if ((req = STAILQ_FIRST(&async_nif->reqs)) == NULL) {
      /* Queue is empty, join the list of idle workers and wait for work */
      enif_cond_wait(async_nif->cnd, async_nif->req_mutex);
      goto check_again_for_work;
    } else {
      /* `req` is our work request and we hold the req_mutex lock. */
      // TODO: do we need this? enif_cond_broadcast(async_nif->cnd);

      /* Remove this thread from the list of idle threads. */
      enif_mutex_lock(async_nif->worker_mutex);
      LIST_REMOVE(worker, entries);
      enif_mutex_unlock(async_nif->worker_mutex);

      do {
        /* Take the request off the queue. */
        STAILQ_REMOVE(&async_nif->reqs, req, async_nif->req_entry, entries);
	async_nif->req_count--;
        enif_mutex_unlock(async_nif->req_mutex);

        /* Finally, do the work. */
        unsigned int worker_id = (unsigned int)(worker - worker_entries);
        req->fn_work(req->env, req->ref, &req->pid, worker_id, req->args);
        req->fn_post(req->args);
        enif_free(req->args);
        enif_free_env(req->env);
        enif_free(req);

	/* Finally, check the request queue for more work before switching
	   into idle mode. */
	enif_mutex_lock(async_nif->req_mutex);
	if ((req = STAILQ_FIRST(&async_nif->reqs)) == NULL) {
	    enif_mutex_unlock(async_nif->req_mutex);
	}

        /* Take a second to see if we need to adjust the number of active
           worker threads up or down. */
        // TODO: if queue_depth > last_depth && num_workers < MAX, start one up

      } while(req);
    }
  }
  enif_thread_exit(0);
  return 0;
}

static void async_nif_unload(ErlNifEnv *env)
{
  unsigned int i;
  struct_nif_state *async_nif = (struct async_nif_state*)enif_priv_data(env);

  /* Signal the worker threads, stop what you're doing and exit. */
  enif_mutex_lock(async_nif->req_mutex);
  async_nif->shutdown = 1;
  enif_cond_broadcast(async_nif->cnd);
  enif_mutex_unlock(async_nif->req_mutex);

  /* Join for the now exiting worker threads. */
  for (i = 0; i < ASYNC_NIF_MAX_WORKERS; ++i) {
    void *exit_value = 0; /* Ignore this. */
    enif_thread_join(async_nif->worker_entries[i].tid, &exit_value);
  }

  /* We won't get here until all threads have exited.
     Patch things up, and carry on. */
  enif_mutex_lock(async_nif->req_mutex);

  /* Worker threads are stopped, now toss anything left in the queue. */
  struct async_nif_req_entry *req = NULL;
  STAILQ_FOREACH(req, &async_nif->reqs, entries) {
    STAILQ_REMOVE(&async_nif->reqs, STAILQ_LAST(&async_nif->reqs, async_nif_req_entry, entries),
                  async_nif_req_entry, entries);
    enif_send(NULL, &req->pid, req->env,
              enif_make_tuple2(req->env, enif_make_atom(req->env, "error"),
                               enif_make_atom(req->env, "shutdown")));
    req->fn_post(req->args);
    enif_free(req->args);
    enif_free(req);
    async_nif->req_count--;
  }
  enif_mutex_unlock(async_nif->req_mutex);

  bzero(async_nif->worker_entries, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS);
  enif_cond_destroy(async_nif->cnd); async_nif->cnd = NULL;
  enif_mutex_destroy(async_nif->req_mutex); async_nif->req_mutex = NULL;
  enif_mutex_destroy(async_nif->worker_mutex); async_nif->worker_mutex = NULL;
  bzero(async_nif, sizeof(struct async_nif_state));
  free(async_nif);
}

static void *
async_nif_load(void)
{
  int i, num_schedulers;
  ErlDrvSysInfo info;
  struct async_nif_state *async_nif;

  /* Don't init more than once. */
  if (async_nif_req_mutex) return 0;

  /* Find out how many schedulers there are. */
  erl_drv_sys_info(&info, sizeof(ErlDrvSysInfo));
  num_schedulers = info->scheduler_threads;

  /* Init our portion of priv_data's module-specific state. */
  async_nif = malloc(sizeof(struct async_nif_state));
  if (!async_nif)
      return NULL;
  STAILQ_INIT(async_nif->reqs);
  LIST_INIT(async_nif->workers);
  async_nif->shutdown = 0;

  async_nif->req_mutex = enif_mutex_create(NULL);
  async_nif->worker_mutex = enif_mutex_create(NULL);
  async_nif->cnd = enif_cond_create(NULL);

  /* Setup the requests management. */
  async_nif->req_count = 0;

  /* Setup the thread pool management. */
  enif_mutex_lock(async_nif->worker_mutex);
  bzero(async_nif->worker_entries, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS);

  /* Start the minimum of max workers allowed or number of scheduler threads running. */
  unsigned int num_worker_threads = ASYNC_NIF_MAX_WORKERS;
  if (num_schedulers < ASYNC_NIF_MAX_WORKERS)
      num_worker_threads = num_schedulers;
  if (num_worker_threads < 1)
      num_worker_threads = 1;

  for (i = 0; i < num_worker_threads; i++) {
    if (enif_thread_create(NULL, &async_nif->worker_entries[i].tid,
                            &async_nif_worker_fn, (void*)&async_nif->worker_entries[i], NULL) != 0) {
      async_nif->shutdown = 1;
      enif_cond_broadcast(async_nif->cnd);
      enif_mutex_unlock(async_nif->worker_mutex);
      while(i-- > 0) {
        void *exit_value = 0; /* Ignore this. */
        enif_thread_join(async_nif->worker_entries[i].tid, &exit_value);
      }
      bzero(async_nif->worker_entries, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS);
      enif_cond_destroy(async_nif->cnd);
      async_nif->cnd = NULL;
      enif_mutex_destroy(async_nif->req_mutex);
      async_nif->req_mutex = NULL;
      enif_mutex_destroy(async_nif->worker_mutex);
      async_nif->worker_mutex = NULL;
      return NULL;
    }
  }
  async_nif->num_workers = num_worker_threads;
  enif_mutex_unlock(async_nif->worker_mutex);
  return async_nif;
}

#if defined(__cplusplus)
}
#endif

#endif // __ASYNC_NIF_H__
