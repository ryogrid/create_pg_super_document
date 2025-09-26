# StartupProcessMain

## Location
[src/backend/postmaster/startup.c:216-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L216-L267)

## Overview
StartupProcessMain is the main entry point for PostgreSQL's startup process, responsible for initializing the process environment, setting up signal handlers, and executing crash recovery through the StartupXLOG function.

## Definition
```c
void StartupProcessMain(char *startup_data, size_t startup_data_len)
```

## Detailed Description
StartupProcessMain serves as the primary entry point for the startup process in PostgreSQL, which is responsible for performing crash recovery when the database starts. The function sets up the complete execution environment for the startup process, including backend type identification, signal handling configuration, timeout registration for standby operations, and finally invokes the core recovery logic through StartupXLOG(). This process is critical for ensuring database consistency after crashes or during standby server operations.

The function follows a structured initialization pattern: it first establishes the process identity and common auxiliary process setup, then configures comprehensive signal handling for various operational scenarios, registers timeouts for standby mode operations, unblocks signals, and finally performs the actual recovery work before exiting successfully.

## Parameters / Member Variables
- `startup_data`: Additional startup data (currently unused, asserted to be empty)
- `startup_data_len`: Length of startup data (currently expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - [StartupXLOG](StartupXLOG.md) (main recovery function)
  - [on_shmem_exit](../o/on_shmem_exit.md) (registers StartupProcExit as exit handler)
  - [pqsignal](../p/pqsignal.md) (signal handler registration)
  - [InitializeTimeouts](../I/InitializeTimeouts.md)
  - [RegisterTimeout](../R/RegisterTimeout.md) (for standby timeouts)
  - sigprocmask
  - [proc_exit](../p/proc_exit.md)
- Signal handlers registered:
  - [StartupProcSigHupHandler](StartupProcSigHupHandler.md) (SIGHUP)
  - [StartupProcShutdownHandler](StartupProcShutdownHandler.md) (SIGTERM)
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md) (SIGUSR1)
  - [StartupProcTriggerHandler](StartupProcTriggerHandler.md) (SIGUSR2)
- Called from (representative examples):
  - launch_backend.c (process launching infrastructure)

## Notes and Other Information
- This function sets MyBackendType to B_STARTUP to identify the process type
- Comprehensive signal handling covers configuration reload, shutdown requests, and standby-specific triggers
- Three standby-related timeouts are registered: deadlock, general standby, and lock timeouts
- The function exits with code 0 upon successful completion, indicating successful recovery to the postmaster
- The startup process is a critical component of PostgreSQL's crash recovery and standby server functionality
- Signal handling is carefully configured to ignore some signals (SIGINT, SIGPIPE) while properly handling others