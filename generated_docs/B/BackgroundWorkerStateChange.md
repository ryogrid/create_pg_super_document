# BackgroundWorkerStateChange

## Location
[src/backend/postmaster/bgworker.c:246-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L246-L431)

## Overview
Processes changes to background worker state in shared memory, handling new worker registrations and termination requests while running in the postmaster process.

## Definition

```c
void
BackgroundWorkerStateChange(bool allow_new_workers)
```
## Detailed Description
This critical function runs in the postmaster process to synchronize the postmaster's private worker list with changes made to shared memory by other backends. It iterates through all worker slots in shared memory, detecting newly registered workers or workers marked for termination. The function employs defensive programming practices to handle potentially corrupted shared memory safely, as a rogue backend could compromise the postmaster if trust is placed in shared memory contents. For new workers, it validates and copies registration data to the postmaster's private list using safe string copying functions. For terminated workers, it handles cleanup and notification procedures.

## Parameters / Member Variables
- : Boolean flag controlling whether new worker registrations should be accepted (false during shutdown scenarios)

## Dependencies
- Functions called/Symbols referenced:
  -  (locates workers by slot number)
  -  (memory barrier for safe shared memory reads)
  -  (full memory barrier)
  -  (sends signals to processes)
  -  (reports worker status)
  -  (safe memory allocation)
  -  (safe string copying)
  -  (validates notification PIDs)
  -  (adds workers to private list)
  -  and  (error reporting)
  -  (memory copying)
  - Constants: , , , , 
  - Memory context flags: , 
  - Global variables: , , , 

- Called from (representative examples):
  -  (src/backend/postmaster/postmaster.c:3771)

## Notes and Other Information
- Implements defensive programming against shared memory corruption
- Uses memory barriers to ensure proper ordering of shared memory operations
- Handles parallel worker tracking with separate terminate count
- Validates notification PIDs against known backend processes
- Employs safe string copying to prevent buffer overflows from corrupted data
- Logs worker registration events at DEBUG1 level
- Critical for postmaster's worker lifecycle management
- Only allocates memory when necessary and handles allocation failures gracefully
- Maintains consistency between shared memory slots and private worker list

## Simplified Source

```c
// Simplified version of BackgroundWorkerStateChange
void BackgroundWorkerStateChange(bool allow_new_workers) {
    int slotno;

    // Sanity check: ensure shared memory matches our expectations
    if (max_worker_processes != BackgroundWorkerData->total_slots) {
        ereport(LOG, (errmsg("inconsistent background worker state")));
        return;
    }

    // Process each worker slot in shared memory
    for (slotno = 0; slotno < max_worker_processes; ++slotno) {
        BackgroundWorkerSlot *slot = &BackgroundWorkerData->slot[slotno];
        RegisteredBgWorker *worker;

        if (!slot->in_use)
            continue;

        // Memory barrier to ensure we see updated slot contents
        pg_read_barrier();

        // Check if we already know about this worker
        worker = FindRegisteredWorkerBySlotNumber(slotno);
        if (worker != NULL) {
            // Handle termination request for existing worker
            if (slot->terminate && !worker->rw_terminate) {
                worker->rw_terminate = true;
                if (worker->rw_pid != 0)
                    kill(worker->rw_pid, SIGTERM);
                else
                    ReportBackgroundWorkerPID(worker);
            }
            continue;
        }

        // Mark new workers for termination if not accepting new workers
        if (!allow_new_workers)
            slot->terminate = true;

        // Handle terminated workers: clean up and notify
        if (slot->terminate) {
            int notify_pid = slot->worker.bgw_notify_pid;

            // Update parallel worker count if needed
            if ((slot->worker.bgw_flags & BGWORKER_CLASS_PARALLEL) != 0)
                BackgroundWorkerData->parallel_terminate_count++;

            // Free the slot
            slot->pid = 0;
            pg_memory_barrier();
            slot->in_use = false;

            // Notify the requesting process
            if (notify_pid != 0)
                kill(notify_pid, SIGUSR1);
            continue;
        }

        // Register new worker: allocate memory and copy data
        worker = MemoryContextAllocExtended(PostmasterContext,
                                          sizeof(RegisteredBgWorker),
                                          MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
        if (worker == NULL) {
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
            return;
        }

        // Safely copy worker configuration from shared memory
        ascii_safe_strlcpy(worker->rw_worker.bgw_name, slot->worker.bgw_name, BGW_MAXLEN);
        ascii_safe_strlcpy(worker->rw_worker.bgw_type, slot->worker.bgw_type, BGW_MAXLEN);
        ascii_safe_strlcpy(worker->rw_worker.bgw_library_name, slot->worker.bgw_library_name, MAXPGPATH);
        ascii_safe_strlcpy(worker->rw_worker.bgw_function_name, slot->worker.bgw_function_name, BGW_MAXLEN);

        // Copy worker configuration fields
        worker->rw_worker.bgw_flags = slot->worker.bgw_flags;
        worker->rw_worker.bgw_start_time = slot->worker.bgw_start_time;
        worker->rw_worker.bgw_restart_time = slot->worker.bgw_restart_time;
        worker->rw_worker.bgw_main_arg = slot->worker.bgw_main_arg;
        memcpy(worker->rw_worker.bgw_extra, slot->worker.bgw_extra, BGW_EXTRALEN);

        // Set up notification PID if valid
        worker->rw_worker.bgw_notify_pid = slot->worker.bgw_notify_pid;
        if (!PostmasterMarkPIDForWorkerNotify(worker->rw_worker.bgw_notify_pid))
            worker->rw_worker.bgw_notify_pid = 0;

        // Initialize postmaster tracking fields
        worker->rw_backend = NULL;
        worker->rw_pid = 0;
        worker->rw_child_slot = 0;
        worker->rw_crashed_at = 0;
        worker->rw_shmem_slot = slotno;
        worker->rw_terminate = false;

        // Add to the postmaster's worker list
        ereport(DEBUG1, (errmsg_internal("registering background worker \"%s\"",
                                        worker->rw_worker.bgw_name)));
        slist_push_head(&BackgroundWorkerList, &worker->rw_lnode);
    }
}
```

Key simplifications made:
- Consolidated error handling and reduced verbose comments
- Streamlined the main loop logic flow
- Abstracted detailed memory barrier explanations
- Simplified variable naming for clarity
- Focused on the three main operations: existing worker termination, terminated worker cleanup, and new worker registration
- Removed detailed commentary about shared memory corruption handling while preserving the safety mechanisms