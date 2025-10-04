# pq_set_parallel_leader

## Location
[src/backend/libpq/pqmq.c:78-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqmq.c#L78-L85)

## Overview
Configures the parallel leader process information that will be signaled each time message data is transmitted via the shared memory message queue.

## Definition
```c
void pq_set_parallel_leader(pid_t pid, ProcNumber procNumber)
```

## Detailed Description
This function sets up the process identification information for the parallel leader process that needs to be notified when a parallel worker sends data through the shared memory message queue. By storing both the process ID and the PostgreSQL process number, it enables the message queue communication system to signal the leader process efficiently when new data is available.

The function includes an assertion to ensure that the communication method has been properly set to use message queues before attempting to configure the parallel leader information.

## Parameters / Member Variables
- `pid`: Process ID of the parallel leader process that should be signaled
- `procNumber`: PostgreSQL internal process number of the parallel leader

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - PqCommMqMethods
  - pq_mq_parallel_leader_pid (global variable)
  - pq_mq_parallel_leader_proc_number (global variable)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md)

## Notes and Other Information
- Must be called after pq_redirect_to_shm_mq has been invoked
- The assertion ensures proper initialization order in the parallel worker setup
- Stores both OS-level process ID and PostgreSQL internal process number for efficient signaling
- This information is used by the message queue flush operations to notify the leader
- Critical for the proper functioning of parallel query execution and result communication
- The stored information enables SendProcSignal() calls to notify the leader of available data

## Simplified Source

```c
void pq_set_parallel_leader(pid_t pid, ProcNumber procNumber) {
    // Ensure we're using message queue communication
    Assert(PqCommMethods == &PqCommMqMethods);

    // Store the parallel leader's process identification
    pq_mq_parallel_leader_pid = pid;
    pq_mq_parallel_leader_proc_number = procNumber;
}
```