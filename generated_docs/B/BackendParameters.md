# BackendParameters

## Location
[src/backend/postmaster/launch_backend.c:157-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L157-L158)

## Overview
BackendParameters is a comprehensive structure that contains all variables and state information needed to launch and initialize backend processes in PostgreSQL. It serves as the primary data container for passing critical system state from the postmaster to newly created backend processes.

## Definition

```c
typedef struct
{
	const char *name;
	void		(*main_fn) (char *startup_data, size_t startup_data_len) pg_attribute_noreturn();
	bool		shmem_attach;
} child_process_kind;
```
## Detailed Description
BackendParameters is the central structure used in PostgreSQL's process forking mechanism to transfer all essential state information from the postmaster process to newly created backend processes. This structure contains everything a backend process needs to initialize itself properly, including shared memory references, locking primitives, configuration flags, and communication channels.

The structure is populated by save_backend_variables() in the parent process and then passed to the child process, where restore_backend_variables() reconstructs the backend's environment. This mechanism is crucial for both fork-based process creation on Unix and the more complex process creation on Windows where full state must be explicitly transferred.

## Parameters / Member Variables
- `DataDir`: PostgreSQL data directory path
- `MyCancelKey`: Unique key for query cancellation requests
- `MyPMChildSlot`: Slot number in the postmaster's child process array
- `UsedShmemSegID`: Shared memory segment identifier (platform-specific)
- `UsedShmemSegAddr`: Windows-specific shared memory protection region
- `ShmemLoc`: Address of the shared memory segment
- `ShmemLock`: Pointer to shared memory lock structure
- `ShmemBackendArray`: Array of backend information in shared memory
- `InjectionPointAttached`: Injection points for testing (optional)
- `SpinlockSemaArray`: Semaphore array for spinlock implementation (when needed)
- `NamedLWLockTrancheRequests`: Number of named lightweight lock tranche requests
- `NamedLWLockTrancheArray`: Array of named lightweight lock tranches
- `MainLWLockArray`: Main lightweight lock array
- `ProcStructLock`: Lock for process structure modifications
- `ProcGlobal`: Global process header information
- `AuxiliaryProcs`: Array of auxiliary process structures
- `PreparedXacts`: Array of prepared transaction process structures
- `PMSignalState`: Postmaster signal communication state
- `PostmasterPid`: Process ID of the postmaster
- `PgStartTime`: PostgreSQL server start time
- `PgReloadTime`: Last configuration reload time
- `first_syslogger_file_time`: System logger first file timestamp
- `redirection_done`: Flag indicating if log redirection is complete
- `IsBinaryUpgrade`: Flag for binary upgrade mode
- `query_id_enabled`: Flag for query ID tracking
- `max_safe_fds`: Maximum safe file descriptors
- `MaxBackends`: Maximum number of backend processes
- `PostmasterHandle`: Windows postmaster process handle
- `postmaster_alive_fds`: Initial signal communication pipe
- `syslogPipe`: System logging pipe handles/descriptors
- `postmaster_alive_fds`: File descriptors for postmaster aliveness detection
- `my_exec_path`: Path to PostgreSQL executable
- `pkglib_path`: Path to PostgreSQL library directory
- `MyProcPort`: Client socket information
- `childsock`: Inheritable socket for the client connection
- `startup_data_len`: Length of additional startup data
- `startup_data`: Flexible array member for process-specific startup data

## Dependencies
- Functions called/Symbols referenced:
  - [ClientSocket](../C/ClientSocket.md)
  - [InheritableSocket](../I/InheritableSocket.md)
  - MAXPGPATH
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [save_backend_variables](../s/save_backend_variables.md)
  - [read_backend_variables](../r/read_backend_variables.md)
  - [restore_backend_variables](../r/restore_backend_variables.md)
  - [internal_forkexec](../i/internal_forkexec.md)

## Notes and Other Information
- Contains platform-specific fields for Windows and Unix systems
- Uses flexible array member for variable-length startup data
- Size calculated using SizeOfBackendParameters macro
- Critical for PostgreSQL's multi-process architecture
- Ensures consistent state transfer during process creation
- Contains both shared memory pointers and configuration flags
- Part of the backend launch infrastructure in launch_backend.c