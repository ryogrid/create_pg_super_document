# LogicalRepCtxStruct

## Location
src/backend/replication/logical/launcher.c: 56 - 67

## Overview
LogicalRepCtxStruct is a shared memory structure that manages the logical replication launcher context, coordinating the supervisor process and background worker processes for PostgreSQL's logical replication system.

## Definition

```c
typedef struct LogicalRepCtxStruct
{
	/* Supervisor process. */
	pid_t		launcher_pid;

	/* Hash table holding last start times of subscriptions' apply workers. */
	dsa_handle	last_start_dsa;
	dshash_table_handle last_start_dsh;

	/* Background workers. */
	LogicalRepWorker workers[FLEXIBLE_ARRAY_MEMBER];
} LogicalRepCtxStruct;
```
## Detailed Description
LogicalRepCtxStruct serves as the central coordination structure for PostgreSQL's logical replication launcher subsystem. It maintains information about the launcher supervisor process, tracks worker start times to implement rate limiting and restart policies, and provides an array of worker slots for managing logical replication apply workers. The structure is allocated in shared memory to allow coordination between the launcher process and multiple worker processes.

The structure uses dynamic shared memory areas (DSA) to maintain a hash table that tracks the last start times of subscription apply workers, which is essential for implementing proper restart throttling and avoiding rapid restart loops when workers encounter errors.

## Parameters / Member Variables
- : Process ID of the logical replication launcher supervisor process
- : Handle to the dynamic shared memory area containing the hash table for worker start times
- : Handle to the dynamic shared hash table that stores last start times of subscriptions' apply workers
- : Flexible array member containing LogicalRepWorker structures representing background worker processes

## Dependencies
- Functions called/Symbols referenced:
  - pid_t
  - dsa_handle
  - dshash_table_handle
  - LogicalRepWorker
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - ApplyLauncherShmemSize
  - ApplyLauncherShmemInit

## Notes and Other Information
- Located in src/backend/replication/logical/launcher.c:56-67
- This structure is allocated in shared memory to enable coordination between launcher and worker processes
- The flexible array member allows for dynamic sizing based on max_logical_replication_workers configuration
- The hash table mechanism helps prevent worker thrashing by tracking restart timing