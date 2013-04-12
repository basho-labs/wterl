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
  ErlNifCond *cnd;
  STAILQ_HEAD(reqs, async_nif_req_entry) reqs;
  unsigned int num_workers;
  struct async_nif_worker_entry worker_entries[ASYNC_NIF_MAX_WORKERS];
};

struct async_nif_worker_info {
  struct async_nif_state *async_nif;
  struct async_nif_worker_entry *worker;
  unsigned int worker_id;
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
    struct async_nif_state *async_nif = (struct async_nif_state*)enif_priv_data(env); \
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
    bzero(req, sizeof(struct async_nif_req_entry));                     \
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

#define ASYNC_NIF_INIT(name)                                    \
        static ErlNifMutex *name##_async_nif_coord = NULL;

#define ASYNC_NIF_LOAD(name, priv) do {                                 \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create(NULL);           \
        enif_mutex_lock(name##_async_nif_coord);                        \
        priv = async_nif_load();                                        \
        enif_mutex_unlock(name##_async_nif_coord);                      \
    } while(0);
#define ASYNC_NIF_UNLOAD(name, env) do {                                \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create(NULL);           \
        enif_mutex_lock(name##_async_nif_coord);                        \
        async_nif_unload(env);                                          \
        enif_mutex_unlock(name##_async_nif_coord);                      \
        enif_mutex_destroy(name##_async_nif_coord);                     \
        name##_async_nif_coord = NULL;                                  \
    } while(0);
#define ASYNC_NIF_UPGRADE(name, env) do {                               \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create(NULL);           \
        enif_mutex_lock(name##_async_nif_coord);                        \
        async_nif_upgrade(env);                                         \
        enif_mutex_unlock(name##_async_nif_coord);                      \
    } while(0);

#define ASYNC_NIF_RETURN_BADARG() return enif_make_badarg(env);
#define ASYNC_NIF_WORK_ENV new_env

#define ASYNC_NIF_REPLY(msg) enif_send(NULL, pid, env, enif_make_tuple2(env, ref, msg))

static ERL_NIF_TERM
async_nif_enqueue_req(struct async_nif_state* async_nif, struct async_nif_req_entry *req)
{
  /* If we're shutting down return an error term and ignore the request. */
  if (async_nif->shutdown) {
    return enif_make_tuple2(req->env, enif_make_atom(req->env, "error"),
			    enif_make_atom(req->env, "shutdown"));
  }

  /* Otherwise, add the request to the work queue. */
  enif_mutex_lock(async_nif->req_mutex);
  STAILQ_INSERT_TAIL(&async_nif->reqs, req, entries);
  async_nif->req_count++;
  /* Build the term before releasing the lock so as not to race on the use of
     the req pointer. */
  ERL_NIF_TERM reply = enif_make_tuple2(req->env, enif_make_atom(req->env, "ok"),
                          enif_make_tuple2(req->env, enif_make_atom(req->env, "enqueued"),
					   enif_make_int(req->env, async_nif->req_count)));
  enif_mutex_unlock(async_nif->req_mutex);
  enif_cond_broadcast(async_nif->cnd);
  return reply;
}

static void *
async_nif_worker_fn(void *arg)
{
  struct async_nif_worker_info *wi = (struct async_nif_worker_info *)arg;
  struct async_nif_state *async_nif = wi->async_nif;
  unsigned int worker_id = wi->worker_id;

  enif_free(arg); // Allocated when starting the thread, now no longer needed.

  for(;;) {
    struct async_nif_req_entry *req = NULL;

    /* Examine the request queue, are there things to be done? */
    enif_mutex_lock(async_nif->req_mutex);
    check_again_for_work:
    if (async_nif->shutdown) {
        enif_mutex_unlock(async_nif->req_mutex);
        break;
    }
    if ((req = STAILQ_FIRST(&async_nif->reqs)) == NULL) {
      /* Queue is empty, wait for work */
      enif_cond_wait(async_nif->cnd, async_nif->req_mutex);
      goto check_again_for_work;
    } else {
      /* `req` is our work request and we hold the req_mutex lock. */

      do {
        /* Take the request off the queue. */
        STAILQ_REMOVE(&async_nif->reqs, req, async_nif_req_entry, entries);
	async_nif->req_count--;
        enif_mutex_unlock(async_nif->req_mutex);

        /* Finally, do the work. */
        req->fn_work(req->env, req->ref, &req->pid, worker_id, req->args);
        req->fn_post(req->args);
        enif_free(req->args);
        enif_free_env(req->env);
        enif_free(req);

        /* Review the work queue, start more worker threads if they are needed. */
        // TODO: if queue_depth > last_depth && num_workers < MAX, start one up

	/* Continue working if more requests are in the queue, otherwise wait
           for new work to arrive. */
	enif_mutex_lock(async_nif->req_mutex);
	if ((req = STAILQ_FIRST(&async_nif->reqs)) == NULL) {
	    enif_mutex_unlock(async_nif->req_mutex);
	}

      } while(req);
    }
  }
  enif_thread_exit(0);
  return 0;
}

static void
async_nif_unload(ErlNifEnv *env)
{
  unsigned int i;
  struct async_nif_state *async_nif = (struct async_nif_state*)enif_priv_data(env);

  /* Signal the worker threads, stop what you're doing and exit. */
  enif_mutex_lock(async_nif->req_mutex);
  async_nif->shutdown = 1;
  enif_cond_broadcast(async_nif->cnd);
  enif_mutex_unlock(async_nif->req_mutex);

  /* Join for the now exiting worker threads. */
  for (i = 0; i < async_nif->num_workers; ++i) {
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
  enif_cond_destroy(async_nif->cnd);
  async_nif->cnd = NULL;
  enif_mutex_destroy(async_nif->req_mutex);
  async_nif->req_mutex = NULL;
  bzero(async_nif, sizeof(struct async_nif_state));
  enif_free(async_nif);
}

static void *
async_nif_load(void)
{
  static int has_init = 0;
  int i, num_schedulers;
  ErlNifSysInfo info;
  struct async_nif_state *async_nif;

  /* Don't init more than once. */
  if (has_init) return 0;
  else has_init = 1;

  /* Find out how many schedulers there are. */
  enif_system_info(&info, sizeof(ErlNifSysInfo));
  num_schedulers = info.scheduler_threads;

  /* Init our portion of priv_data's module-specific state. */
  async_nif = enif_alloc(sizeof(struct async_nif_state));
  if (!async_nif)
      return NULL;
  bzero(async_nif, sizeof(struct async_nif_state));
  STAILQ_INIT(&(async_nif->reqs));
  async_nif->shutdown = 0;

  async_nif->req_mutex = enif_mutex_create(NULL);
  async_nif->cnd = enif_cond_create(NULL);

  /* Setup the requests management. */
  async_nif->req_count = 0;

  /* Setup the thread pool management. */
  bzero(async_nif->worker_entries, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS);

  /* Start the minimum of max workers allowed or number of scheduler threads running. */
  unsigned int num_worker_threads = ASYNC_NIF_MAX_WORKERS;
  if (num_schedulers < ASYNC_NIF_MAX_WORKERS)
      num_worker_threads = num_schedulers;
  if (num_worker_threads < 1)
      num_worker_threads = 1;
  num_worker_threads = ASYNC_NIF_MAX_WORKERS; // TODO: make this dynamic at some point

  for (i = 0; i < num_worker_threads; i++) {
    struct async_nif_worker_info *wi;
    wi = enif_alloc(sizeof(struct async_nif_worker_info)); // TODO: check
    bzero(wi, sizeof(struct async_nif_worker_info));
    wi->async_nif = async_nif;
    wi->worker = &async_nif->worker_entries[i];
    wi->worker_id = i;
    if (enif_thread_create(NULL, &async_nif->worker_entries[i].tid,
                            &async_nif_worker_fn, (void*)wi, NULL) != 0) {
      async_nif->shutdown = 1;
      enif_cond_broadcast(async_nif->cnd);
      while(i-- > 0) {
        void *exit_value = 0; /* Ignore this. */
        enif_thread_join(async_nif->worker_entries[i].tid, &exit_value);
      }
      bzero(async_nif->worker_entries, sizeof(struct async_nif_worker_entry) * ASYNC_NIF_MAX_WORKERS);
      enif_cond_destroy(async_nif->cnd);
      async_nif->cnd = NULL;
      enif_mutex_destroy(async_nif->req_mutex);
      async_nif->req_mutex = NULL;
      return NULL;
    }
  }
  async_nif->num_workers = i;
  return async_nif;
}

static void
async_nif_upgrade(ErlNifEnv *env)
{
    // TODO:
}


#if defined(__cplusplus)
}
#endif

#endif // __ASYNC_NIF_H__
