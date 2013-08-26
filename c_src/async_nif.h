/*
 * async_nif: An async thread-pool layer for Erlang's NIF API
 *
 * Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 * Author: Gregory Burd <greg@basho.com> <greg@burd.me>
 *
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#ifndef __ASYNC_NIF_H__
#define __ASYNC_NIF_H__

#if defined(__cplusplus)
extern "C" {
#endif

#include <assert.h>
#include <urcu.h>
#include <urcu/cds.h>
#include <urcu-defer.h>
#include <urcu/arch.h>
#include <urcu/tls-compat.h>

#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif

#define ASYNC_NIF_MAX_WORKERS 1024
#define ASYNC_NIF_MIN_WORKERS 2
#define ASYNC_NIF_WORKER_QUEUE_SIZE 8192
#define ASYNC_NIF_MAX_QUEUED_REQS ASYNC_NIF_WORKER_QUEUE_SIZE * ASYNC_NIF_MAX_WORKERS

static DEFINE_URCU_TLS(unsigned long long, nr_enqueues);
static DEFINE_URCU_TLS(unsigned long long, nr_dequeues);

/* Atoms (initialized in on_load) */
static ERL_NIF_TERM ATOM_EAGAIN;
static ERL_NIF_TERM ATOM_ENOMEM;
static ERL_NIF_TERM ATOM_ENQUEUED;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_SHUTDOWN;

struct async_nif_req_entry {
  ERL_NIF_TERM ref;
  ErlNifEnv *env;
  ErlNifPid pid;
  void *args;
  void (*fn_work)(ErlNifEnv*, ERL_NIF_TERM, ErlNifPid*, unsigned int, void *);
  void (*fn_post)(void *);
  struct cds_lfq_node_rcu queue_entry;
};

struct async_nif_work_queue {
  unsigned int num_workers;
  unsigned int depth;
  struct cds_lfq_queue_rcu req_queue;
  struct async_nif_work_queue *next;
};

struct async_nif_worker_entry {
  ErlNifTid tid;
  unsigned int worker_id;
  struct async_nif_state *async_nif;
  struct async_nif_work_queue *q;
  struct cds_lfq_node_rcu queue_entry;
};

struct async_nif_state {
  unsigned int shutdown;
  unsigned int num_active_workers;
  struct cds_lfq_queue_rcu worker_join_queue;
  unsigned int num_queues;
  unsigned int next_q;
  struct cds_lfq_queue_rcu recycled_req_queue;
  unsigned int num_reqs;
  struct rcu_head rcu;
  struct async_nif_work_queue queues[];
};

#define ASYNC_NIF_DECL(decl, frame, pre_block, work_block, post_block)  \
  struct decl##_args frame;                                             \
  static void fn_work_##decl (ErlNifEnv *env, ERL_NIF_TERM ref, ErlNifPid *pid, unsigned int worker_id, struct decl##_args *args) { \
  UNUSED(worker_id);                                                    \
  DPRINTF("async_nif: calling \"%s\"", __func__);                       \
  do work_block while(0);                                               \
  DPRINTF("async_nif: returned from \"%s\"", __func__);                 \
  }                                                                     \
  static void fn_post_##decl (struct decl##_args *args) {               \
    UNUSED(args);                                                       \
    DPRINTF("async_nif: calling \"fn_post_%s\"", #decl);                \
    do post_block while(0);                                             \
    DPRINTF("async_nif: returned from \"fn_post_%s\"", #decl);          \
  }                                                                     \
  static ERL_NIF_TERM decl(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv_in[]) { \
    struct decl##_args on_stack_args;                                   \
    struct decl##_args *args = &on_stack_args;                          \
    struct decl##_args *copy_of_args;                                   \
    struct async_nif_req_entry *req = NULL;                             \
    unsigned int affinity = 0;                                          \
    ErlNifEnv *new_env = NULL;                                          \
    /* argv[0] is a ref used for selective recv */                      \
    const ERL_NIF_TERM *argv = argv_in + 1;                             \
    argc -= 1;                                                          \
    /* Note: !!! this assumes that the first element of priv_data is ours */ \
    struct async_nif_state *async_nif = *(struct async_nif_state**)enif_priv_data(env); \
    if (async_nif->shutdown)						\
	return enif_make_tuple2(env, ATOM_ERROR, ATOM_SHUTDOWN);	\
    req = async_nif_reuse_req(async_nif);                               \
    if (!req)								\
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_ENOMEM);		\
    new_env = req->env;                                                 \
    DPRINTF("async_nif: calling \"%s\"", __func__);                     \
    do pre_block while(0);                                              \
    DPRINTF("async_nif: returned from \"%s\"", __func__);               \
    copy_of_args = (struct decl ## _args *)malloc(sizeof(struct decl ## _args)); \
    if (!copy_of_args) {                                                \
      fn_post_##decl (args);                                            \
      async_nif_recycle_req(req, async_nif);                            \
      return enif_make_tuple2(env, ATOM_ERROR, ATOM_ENOMEM);		\
    }                                                                   \
    memcpy(copy_of_args, args, sizeof(struct decl##_args));             \
    req->ref = enif_make_copy(new_env, argv_in[0]);                     \
    enif_self(env, &req->pid);                                          \
    req->args = (void*)copy_of_args;                                    \
    req->fn_work = (void (*)(ErlNifEnv *, ERL_NIF_TERM, ErlNifPid*, unsigned int, void *))fn_work_##decl ; \
    req->fn_post = (void (*)(void *))fn_post_##decl;                   \
    int h = -1;                                                        \
    if (affinity)                                                      \
        h = ((unsigned int)affinity) % async_nif->num_queues;          \
    ERL_NIF_TERM reply = async_nif_enqueue_req(async_nif, req, h);     \
    if (!reply) {                                                      \
      fn_post_##decl (args);                                           \
      async_nif_recycle_req(req, async_nif);                           \
      free(copy_of_args);					       \
      return enif_make_tuple2(env, ATOM_ERROR, ATOM_EAGAIN);	       \
    }                                                                  \
    return reply;                                                      \
  }

#define ASYNC_NIF_INIT(name)                                            \
        static ErlNifMutex *name##_async_nif_coord = NULL;

#define ASYNC_NIF_LOAD(name, env, priv) do {				\
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create("nif_coord load"); \
        enif_mutex_lock(name##_async_nif_coord);                        \
        priv = async_nif_load(env);					\
        enif_mutex_unlock(name##_async_nif_coord);                      \
    } while(0);
#define ASYNC_NIF_UNLOAD(name, env, priv) do {                          \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create("nif_coord unload"); \
        enif_mutex_lock(name##_async_nif_coord);                        \
        async_nif_unload(env, priv);                                    \
        enif_mutex_unlock(name##_async_nif_coord);                      \
        enif_mutex_destroy(name##_async_nif_coord);                     \
        name##_async_nif_coord = NULL;                                  \
    } while(0);
#define ASYNC_NIF_UPGRADE(name, env) do {                               \
        if (!name##_async_nif_coord)                                    \
            name##_async_nif_coord = enif_mutex_create("nif_coord upgrade"); \
        enif_mutex_lock(name##_async_nif_coord);                        \
        async_nif_upgrade(env);                                         \
        enif_mutex_unlock(name##_async_nif_coord);                      \
    } while(0);

#define ASYNC_NIF_RETURN_BADARG() do {                                  \
        async_nif_recycle_req(req, async_nif);                          \
        return enif_make_badarg(env);                                   \
    } while(0);
#define ASYNC_NIF_WORK_ENV new_env

#define ASYNC_NIF_REPLY(msg) enif_send(NULL, pid, env, enif_make_tuple2(env, ref, msg))

/**
 * Return a request structure from the recycled req queue if one exists,
 * otherwise create one.
 */
struct async_nif_req_entry *
async_nif_reuse_req(struct async_nif_state *async_nif)
{
    struct cds_lfq_node_rcu *node;
    struct async_nif_req_entry *req = NULL;
    ErlNifEnv *env = NULL;

    /* Look for a request on our Lock-Free/RCU Queue first. */
    rcu_read_lock();
    node = cds_lfq_dequeue_rcu(&async_nif->recycled_req_queue);
    req = caa_container_of(node, struct async_nif_req_entry, queue_entry);
    rcu_read_unlock();

    if (req) {
        /* The goal is to reuse these req structs, not malloc/free them
           repeatedly so we don't `call_rcu(&async_nif->rcu, free_req_cb);`.
           We reuse this req, then when exiting we'll free all of them at
           once. */
        return req;
    } else {
        if (uatomic_read(&async_nif->num_reqs) < ASYNC_NIF_MAX_QUEUED_REQS) {
            req = malloc(sizeof(struct async_nif_req_entry));
            if (req) {
                memset(req, 0, sizeof(struct async_nif_req_entry));
                env = enif_alloc_env();
                if (env) {
                    req->env = env;
                    uatomic_inc(&async_nif->num_reqs);
                } else {
                    free(req);
                    req = NULL;
                }
            }
        }
    }
    return req;
}

/**
 * Store the request for future re-use.
 *
 * req         a request entry with an ErlNifEnv* which will be cleared
 *             before reuse, but not until then.
 * async_nif   a handle to our state so that we can find and use the mutex
 */
void
async_nif_recycle_req(struct async_nif_req_entry *req, struct async_nif_state *async_nif)
{
    /* Three things to do here to prepare this request struct for reuse.
       1) clear the NIF Environment
       2) zero out the req struct except...
       3) keep a pointer to the env so we can reset it in the req */
    ErlNifEnv *env = req->env;
    enif_clear_env(req->env);
    if (req->args) free(req->args);
    memset(req, 0, sizeof(struct async_nif_req_entry));
    req->env = env;

    /* Now enqueue this request on our Lock-Free/RCU Queue to be reused later. */
    cds_lfq_node_init_rcu(&req->queue_entry);
    rcu_read_lock();
    cds_lfq_enqueue_rcu(&async_nif->recycled_req_queue, &req->queue_entry);
    rcu_read_unlock();
}

static void *async_nif_worker_fn(void *);

/**
 * Start up a worker thread.
 */
static int
async_nif_start_worker(struct async_nif_state *async_nif, struct async_nif_work_queue *q)
{
  struct async_nif_worker_entry *worker;

  if (0 == q)
      return EINVAL;

  /* Before creating a new worker thread join threads which have exited. */
  for(;;) {
      struct cds_lfq_node_rcu *node;
      rcu_read_lock();
      node = cds_lfq_dequeue_rcu(&async_nif->worker_join_queue);
      worker = caa_container_of(node, struct async_nif_worker_entry, queue_entry);
      rcu_read_unlock();

      if (worker) {
          void *exit_value = 0; /* We ignore the thread_join's exit value. */
          enif_thread_join(worker->tid, &exit_value);
          free(worker);
          uatomic_dec(&async_nif->num_active_workers);
      } else
          break;
  }

  if (uatomic_read(&async_nif->num_active_workers) >= ASYNC_NIF_MAX_WORKERS)
      return EAGAIN;

  worker = malloc(sizeof(struct async_nif_worker_entry));
  if (!worker) return ENOMEM;
  memset(worker, 0, sizeof(struct async_nif_worker_entry));
  worker->worker_id = uatomic_add_return(&async_nif->num_active_workers, 1);
  worker->async_nif = async_nif;
  worker->q = q;
  return enif_thread_create(NULL,&worker->tid, &async_nif_worker_fn, (void*)worker, 0);
}

/**
 * Enqueue a request for processing by a worker thread.
 *
 * Places the request into a work queue determined either by the
 * provided affinity or by iterating through the available queues.
 */
static ERL_NIF_TERM
async_nif_enqueue_req(struct async_nif_state* async_nif, struct async_nif_req_entry *req, int hint)
{
  /* Identify the most appropriate worker for this request. */
  unsigned int i, last_qid, qid = 0;
  struct async_nif_work_queue *q = NULL;
  double avg_depth = 0.0;

  /* Either we're choosing a queue based on some affinity/hinted value or we
     need to select the next queue in the rotation and atomically update that
     global value (next_q is shared across worker threads) . */
  if (hint >= 0) {
      qid = (unsigned int)hint;
  } else {
      do {
          last_qid = __sync_fetch_and_add(&async_nif->next_q, 0);
          qid = (last_qid + 1) % async_nif->num_queues;
      } while (!__sync_bool_compare_and_swap(&async_nif->next_q, last_qid, qid));
  }

  /* Now we inspect and interate across the set of queues trying to select one
     that isn't too full or too slow. */
  for (i = 0; i < async_nif->num_queues; i++) {
      /* Compute the average queue depth not counting queues which are empty or
         the queue we're considering right now. */
      unsigned int j, d, n = 0;
      for (j = 0; j < async_nif->num_queues; j++) {
          d = uatomic_read(&async_nif->queues[j].depth);
          if (j != qid && d != 0) {
              n++;
              avg_depth += d;
          }
      }
      if (avg_depth) avg_depth /= n;

      q = &async_nif->queues[qid];
      if (uatomic_read(&async_nif->shutdown))
          return 0;

      /* Try not to enqueue a request into a queue that isn't keeping up with
         the request volume. */
      if (uatomic_read(&q->depth) <= avg_depth) break;
      else qid = (qid + 1) % async_nif->num_queues;
  }

  /* If the for loop finished then we didn't find a suitable queue for this
     request, meaning we're backed up so trigger eagain. */
  if (i == async_nif->num_queues) return 0;

  /* Add the request to the queue. */
  cds_lfq_node_init_rcu(&req->queue_entry);
  rcu_read_lock();
  cds_lfq_enqueue_rcu(&q->req_queue, &req->queue_entry);
  rcu_read_unlock();
  URCU_TLS(nr_enqueues)++;
  uatomic_inc(&q->depth);

  /* We've selected a queue for this new request now check to make sure there are
     enough workers actively processing requests on this queue. */
  while (uatomic_read(&q->depth) > uatomic_read(&q->num_workers)) {
      switch(async_nif_start_worker(async_nif, q)) {
      case EINVAL: case ENOMEM: default: return 0;
      case EAGAIN: continue;
      case 0:      uatomic_inc(&q->num_workers); goto done;
      }
  } done:;

  /* Build the term before releasing the lock so as not to race on the use of
     the req pointer (which will soon become invalid in another thread
     performing the request). */
  double pct_full = (double)avg_depth / (double)ASYNC_NIF_WORKER_QUEUE_SIZE;
  ERL_NIF_TERM reply = enif_make_tuple2(req->env, ATOM_OK,
					enif_make_tuple2(req->env, ATOM_ENQUEUED,
							 enif_make_double(req->env, pct_full)));
  return reply;
}

/**
 * Worker threads execute this function.  Here each worker pulls requests of
 * their respective queues, executes that work and continues doing that until
 * they see the shutdown flag is set at which point they exit.
 */
static void *
async_nif_worker_fn(void *arg)
{
  struct async_nif_worker_entry *worker = (struct async_nif_worker_entry *)arg;
  unsigned int worker_id = worker->worker_id;
  struct async_nif_state *async_nif = worker->async_nif;
  struct async_nif_work_queue *l = NULL, *q = worker->q;

  // TODO(gburd): set_affinity(); to the CPU_ID for this queue
  rcu_register_thread();

  while(q != l) {
    struct cds_lfq_node_rcu *node;
    struct async_nif_req_entry *req = NULL;

    if (uatomic_read(&async_nif->shutdown))
        break;

    rcu_read_lock();
    node = cds_lfq_dequeue_rcu(&q->req_queue);
    req = caa_container_of(node, struct async_nif_req_entry, queue_entry);
    rcu_read_unlock();

    if (req) {
        uatomic_dec(&q->depth);
        URCU_TLS(nr_dequeues)++;
        req->fn_work(req->env, req->ref, &req->pid, worker_id, req->args);
        req->fn_post(req->args);
        async_nif_recycle_req(req, async_nif);
        l = q;
    } else {
        /* This queue is empty, cycle through other queues looking for work. */
        uatomic_dec(&q->num_workers);
        q = q->next;
        uatomic_inc(&q->num_workers);
    }
  }
  uatomic_dec(&q->num_workers);
  cds_lfq_node_init_rcu(&worker->queue_entry);
  rcu_read_lock();
  cds_lfq_enqueue_rcu(&async_nif->worker_join_queue, &worker->queue_entry);
  rcu_read_unlock();
  rcu_unregister_thread();
  enif_thread_exit(0);
  return 0;
}

static void
async_nif_unload(ErlNifEnv *env, struct async_nif_state *async_nif)
{
  unsigned int i;
  unsigned int num_queues = async_nif->num_queues;
  struct cds_lfq_node_rcu *node;
  struct async_nif_work_queue *q = NULL;
  UNUSED(env);

  /* Signal the worker threads, stop what you're doing and exit. */
  uatomic_set(&async_nif->shutdown, 1);

  /* Join for the now exiting worker threads. */
  while(uatomic_read(&async_nif->num_active_workers) > 0) {
      struct async_nif_worker_entry *worker;
      struct cds_lfq_node_rcu *node;
      rcu_read_lock();
      node = cds_lfq_dequeue_rcu(&async_nif->worker_join_queue);
      worker = caa_container_of(node, struct async_nif_worker_entry, queue_entry);
      rcu_read_unlock();

      if (worker) {
          void *exit_value = 0; /* We ignore the thread_join's exit value. */
          enif_thread_join(worker->tid, &exit_value);
          free(worker);
          uatomic_dec(&async_nif->num_active_workers);
      }
  }
  cds_lfq_destroy_rcu(&async_nif->worker_join_queue); // TODO(gburd): check return val

  /* Cleanup in-flight requests, mutexes and conditions in each work queue. */
  for (i = 0; i < num_queues; i++) {
      q = &async_nif->queues[i];

      /* Worker threads are stopped, now toss anything left in the queue. */
      do {
          node = cds_lfq_dequeue_rcu(&q->req_queue);
          if (node) {
              struct async_nif_req_entry *req;
              req = caa_container_of(node, struct async_nif_req_entry, queue_entry);
              enif_clear_env(req->env);
              enif_send(NULL, &req->pid, req->env, enif_make_tuple2(req->env, ATOM_ERROR, ATOM_SHUTDOWN));
              req->fn_post(req->args);
              free(req->args);
              enif_free_env(req->env);
              free(req);
          }
      } while(node);
      cds_lfq_destroy_rcu(&q->req_queue); // TODO(gburd): check return val
  }

  /* Free any req structures sitting unused on the recycle queue. */
  while ((node = cds_lfq_dequeue_rcu(&async_nif->recycled_req_queue)) != NULL) {
      struct async_nif_req_entry *req;
      req = caa_container_of(node, struct async_nif_req_entry, queue_entry);
      enif_free_env(req->env);
      free(req->args);
      free(req);
  }
  cds_lfq_destroy_rcu(&async_nif->recycled_req_queue);  // TODO(gburd): check return val

  memset(async_nif, 0, sizeof(struct async_nif_state) + (sizeof(struct async_nif_work_queue) * async_nif->num_queues));
  free_all_cpu_call_rcu_data();
  free(async_nif);
}

static void *
async_nif_load(ErlNifEnv *env)
{
  static int has_init = 0;
  unsigned int i, num_queues;
  ErlNifSysInfo info;
  struct async_nif_state *async_nif;

  /* Don't init more than once. */
  if (has_init) return 0;
  else has_init = 1;

  /* Init some static references to commonly used atoms. */
  ATOM_EAGAIN = enif_make_atom(env, "eagain");
  ATOM_ENOMEM = enif_make_atom(env, "enomem");
  ATOM_ENQUEUED = enif_make_atom(env, "enqueued");
  ATOM_ERROR = enif_make_atom(env, "error");
  ATOM_OK = enif_make_atom(env, "ok");
  ATOM_SHUTDOWN = enif_make_atom(env, "shutdown");

  /* Init the RCU library. */
  rcu_init();
  (void)create_all_cpu_call_rcu_data(0);

  /* Find out how many schedulers there are. */
  enif_system_info(&info, sizeof(ErlNifSysInfo));

  /* Size the number of work queues according to schedulers. */
  if (info.scheduler_threads > ASYNC_NIF_MAX_WORKERS / 2) {
      num_queues = ASYNC_NIF_MAX_WORKERS / 2;
  } else {
      int remainder = ASYNC_NIF_MAX_WORKERS % info.scheduler_threads;
      if (remainder != 0)
          num_queues = info.scheduler_threads - remainder;
      else
          num_queues = info.scheduler_threads;
      if (num_queues < 2)
          num_queues = 2;
  }

  /* Init our portion of priv_data's module-specific state. */
  async_nif = malloc(sizeof(struct async_nif_state) +
		     sizeof(struct async_nif_work_queue) * num_queues);
  if (!async_nif)
      return NULL;
  memset(async_nif, 0, sizeof(struct async_nif_state) +
                       sizeof(struct async_nif_work_queue) * num_queues);

  async_nif->num_queues = num_queues;
  async_nif->num_active_workers = 0;
  async_nif->next_q = 0;
  async_nif->shutdown = 0;
  cds_lfq_init_rcu(&async_nif->recycled_req_queue, call_rcu);
  cds_lfq_init_rcu(&async_nif->worker_join_queue, call_rcu);

  for (i = 0; i < async_nif->num_queues; i++) {
      struct async_nif_work_queue *q = &async_nif->queues[i];
      cds_lfq_init_rcu(&q->req_queue, call_rcu);
      q->next = &async_nif->queues[(i + 1) % num_queues];
  }
  return async_nif;
}

static void
async_nif_upgrade(ErlNifEnv *env)
{
     UNUSED(env);
    // TODO:
}


#if defined(__cplusplus)
}
#endif

#endif // __ASYNC_NIF_H__
