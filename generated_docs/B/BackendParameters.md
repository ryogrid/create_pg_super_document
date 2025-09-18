# BackendParameters

## Location
[src/backend/postmaster/launch_backend.c:157-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L157-L158)

## Overview
BackendParameters is a comprehensive structure that contains all variables and state information needed to launch and initialize backend processes in PostgreSQL. It serves as the primary data container for passing critical system state from the postmaster to newly created backend processes.

## Definition


## Detailed Description
BackendParameters is the central structure used in PostgreSQL's process forking mechanism to transfer all essential state information from the postmaster process to newly created backend processes. This structure contains everything a backend process needs to initialize itself properly, including shared memory references, locking primitives, configuration flags, and communication channels.

The structure is populated by save_backend_variables() in the parent process and then passed to the child process, where restore_backend_variables() reconstructs the backend's environment. This mechanism is crucial for both fork-based process creation on Unix and the more complex process creation on Windows where full state must be explicitly transferred.

## Parameters / Member Variables
- : PostgreSQL data directory path
- : Unique key for query cancellation requests
- : Slot number in the postmaster's child process array
- : Shared memory segment identifier (platform-specific)
- : Windows-specific shared memory protection region
- : Address of the shared memory segment
- : Pointer to shared memory lock structure
- : Array of backend information in shared memory
- : Injection points for testing (optional)
- : Semaphore array for spinlock implementation (when needed)
- : Number of named lightweight lock tranche requests
- : Array of named lightweight lock tranches
- : Main lightweight lock array
- : Lock for process structure modifications
- : Global process header information
- : Array of auxiliary process structures
- : Array of prepared transaction process structures
- : Postmaster signal communication state
- : Process ID of the postmaster
- : PostgreSQL server start time
- : Last configuration reload time
- : System logger first file timestamp
- : Flag indicating if log redirection is complete
- : Flag for binary upgrade mode
- : Flag for query ID tracking
- : Maximum safe file descriptors
- : Maximum number of backend processes
- : Windows postmaster process handle
- : Initial signal communication pipe
- : System logging pipe handles/descriptors
- : File descriptors for postmaster aliveness detection
- : Path to PostgreSQL executable
- : Path to PostgreSQL library directory
- : Client socket information
- : Inheritable socket for the client connection
- : Length of additional startup data
- : Flexible array member for process-specific startup data

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