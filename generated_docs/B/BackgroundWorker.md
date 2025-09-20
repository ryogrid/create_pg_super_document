# BackgroundWorker

## Location
[src/include/postmaster/bgworker.h:89-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postmaster/bgworker.h#L89-L101)

## Overview
BackgroundWorker is a structure that defines the configuration and properties of a background worker process in PostgreSQL, containing all necessary information for the postmaster to launch and manage the worker.

## Definition

```c
typedef struct BackgroundWorker
{
	char		bgw_name[BGW_MAXLEN];
	char		bgw_type[BGW_MAXLEN];
	int			bgw_flags;
	BgWorkerStartTime bgw_start_time;
	int			bgw_restart_time;	/* in seconds, or BGW_NEVER_RESTART */
	char		bgw_library_name[MAXPGPATH];
	char		bgw_function_name[BGW_MAXLEN];
	Datum		bgw_main_arg;
	char		bgw_extra[BGW_EXTRALEN];
	pid_t		bgw_notify_pid; /* SIGUSR1 this backend on start/stop */
} BackgroundWorker;
```
## Detailed Description
The BackgroundWorker structure serves as a comprehensive configuration template for background worker processes in PostgreSQL. It encapsulates all the necessary information required by the postmaster to spawn, monitor, and manage background worker processes. This structure is used both for static workers (registered during shared_preload_libraries) and dynamic workers (registered at runtime). The postmaster uses this information to determine when to start the worker, how to restart it upon failure, and which function to execute as the worker's main entry point.

## Parameters / Member Variables
- `bgw_name[BGW_MAXLEN]`: Human-readable name for the background worker, used for identification in logs and process lists
- `bgw_type[BGW_MAXLEN]`: Classification type of the worker, used for grouping and management purposes
- `bgw_flags`: Bitfield controlling worker behavior and capabilities (e.g., database access permissions)
- `bgw_start_time`: Specifies when during PostgreSQL startup the worker should be launched
- `bgw_restart_time`: Interval in seconds for automatic restart after termination, or BGW_NEVER_RESTART to disable
- `bgw_library_name[MAXPGPATH]`: Path to the shared library containing the worker's entry point function
- `bgw_function_name[BGW_MAXLEN]`: Name of the function within the library that serves as the worker's main entry point
- `bgw_main_arg`: Datum argument passed to the worker's main function for initialization
- `bgw_extra[BGW_EXTRALEN]`: Additional configuration data or parameters specific to the worker implementation
- `bgw_notify_pid`: Process ID to notify with SIGUSR1 signal when the worker starts or stops
## Dependencies
- Functions called/Symbols referenced:
  - BGW_MAXLEN
  - [BgWorkerStartTime](BgWorkerStartTime.md)
  - BGW_EXTRALEN
  - pid_t
- Called from (representative examples):
  - [RegisterBackgroundWorker](../R/RegisterBackgroundWorker.md)
  - [RegisterDynamicBackgroundWorker](../R/RegisterDynamicBackgroundWorker.md)
  - [BackgroundWorkerMain](BackgroundWorkerMain.md)
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md)

## Notes and Other Information
The BackgroundWorker structure is the fundamental building block of PostgreSQL's background worker infrastructure. Workers can be registered statically during server startup or dynamically during runtime. The structure's design allows for flexible worker configuration while providing the postmaster with sufficient information for process lifecycle management. The bgw_flags field supports various capabilities like database connectivity, and the restart mechanism enables robust fault tolerance for critical background processes.