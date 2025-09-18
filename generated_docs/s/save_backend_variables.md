# save_backend_variables

## Location
[src/backend/postmaster/launch_backend.c:708-794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L708-L794)

## Overview
Serializes critical PostgreSQL backend variables into a BackendParameters structure for transmission to child processes in EXEC_BACKEND mode.

## Definition
```c
static bool save_backend_variables(BackendParameters *param, ClientSocket *client_sock,
#ifdef WIN32
                                   HANDLE childProcess, pid_t childPid,
#endif
                                   char *startup_data, size_t startup_data_len)
```

## Detailed Description
save_backend_variables is responsible for capturing and serializing the current state of the postmaster process into a BackendParameters structure. This serialized state will be passed to child processes launched via EXEC_BACKEND mode (fork+exec or CreateProcess), allowing them to restore the necessary runtime environment.

The function copies numerous global variables including shared memory pointers, configuration settings, file descriptors, process identifiers, and other critical state information. Special handling is provided for platform-specific elements like socket inheritance (Windows vs Unix) and process handles.

This serialization is essential because EXEC_BACKEND child processes do not inherit the parent's memory space, unlike traditional fork() where children inherit all global state automatically.

## Parameters / Member Variables
- `param`: Destination BackendParameters structure to populate with serialized state
- `client_sock`: Optional client socket information for backend processes handling connections
- `childProcess`: (Windows only) Handle to the child process for handle duplication
- `childPid`: (Windows only) Process ID of the child process
- `startup_data`: Process-specific initialization data to include
- `startup_data_len`: Size of the startup_data buffer

## Dependencies
- Functions called/Symbols referenced:
  - [write_inheritable_socket](../w/write_inheritable_socket.md) (socket inheritance handling)
  - [write_duplicated_handle](../w/write_duplicated_handle.md) (Windows handle duplication)  
  - [pgwin32_create_signal_listener](../p/pgwin32_create_signal_listener.md) (Windows signal handling)
  - strlcpy (safe string copying)
  - memcpy/memset (memory operations)
  - Various global variables (DataDir, MyCancelKey, SharedMemory pointers, etc.)
- Called from (representative examples):
  - [internal_forkexec](../i/internal_forkexec.md) (to serialize state before launching child process)

## Notes and Other Information
- This is a static function only available in EXEC_BACKEND builds
- Returns true on success, false on failure (e.g., socket inheritance issues)
- Handles platform differences between Windows and Unix for socket/handle inheritance
- Serializes over 30 different global variables and system state elements
- The startup_data is appended to the end of the structure using a flexible array member
- Critical for ensuring child processes can restore shared memory connections, file descriptors, and configuration state
- Works in conjunction with restore_backend_variables() which deserializes this data in child processes
- Located in src/backend/postmaster/launch_backend.c:708-794
- Platform-specific code paths handle Windows process/handle management vs Unix file descriptor inheritance