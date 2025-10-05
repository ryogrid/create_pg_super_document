# restore_backend_variables

## Location
[src/backend/postmaster/launch_backend.c:977-1055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L977-L1055)

## Overview
Restores critical backend global variables and shared memory structures from a BackendParameters struct that was passed from the postmaster process.

## Definition
```c
static void restore_backend_variables(BackendParameters *param)
```

## Detailed Description
This function is responsible for restoring the complete state of a backend process by copying values from a BackendParameters structure into the appropriate global variables and shared memory pointers. It handles client socket restoration, shared memory segment information, lock structures, process management data, and various configuration parameters. The function includes platform-specific code for Windows and Unix/Linux differences, and handles conditional compilation for features like injection points and spinlocks.

## Parameters / Member Variables
- `param`: Pointer to a BackendParameters structure containing all the backend state information to restore

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - memcpy
  - [read_inheritable_socket](read_inheritable_socket.md)
  - [SetDataDir](../S/SetDataDir.md)
  - [strlcpy](../s/strlcpy.md)
  - [ReserveExternalFD](../R/ReserveExternalFD.md) (Unix/Linux only)
  - [BackendParameters](../B/BackendParameters.md) (structure type)
  - [ClientSocket](../C/ClientSocket.md) (structure type)
  - PGINVALID_SOCKET (constant)
  - TopMemoryContext (global variable)
  - MAXPGPATH (constant)
- Called from (representative examples):
  - [read_backend_variables](read_backend_variables.md)
  - SizeOfBackendParameters

## Notes and Other Information
- Restores numerous critical global variables including:
  - MyClientSocket: Client connection information
  - DataDir: PostgreSQL data directory path
  - MyCancelKey, MyPMChildSlot: Process identification
  - Shared memory pointers (ShmemLock, ShmemBackendArray, etc.)
  - Process management structures (ProcGlobal, PMSignalState)
  - Configuration flags (IsBinaryUpgrade, query_id_enabled)
- Handles platform-specific variables with conditional compilation
- On Windows: restores PostmasterHandle and initial_signal_pipe
- On Unix/Linux: restores postmaster_alive_fds and manages external FD counting
- Properly handles client socket restoration by allocating new memory and using read_inheritable_socket()
- Restores file descriptor management state by calling ReserveExternalFD() for postmaster alive pipes
- Uses safe string copying (strlcpy) for path variables
- Critical for backend process initialization - ensures the child process has access to all shared resources

## Simplified Source

```c
static void restore_backend_variables(BackendParameters *param) {
    // Restore client socket if valid
    if (param->client_sock.sock != PGINVALID_SOCKET) {
        MyClientSocket = MemoryContextAlloc(TopMemoryContext, sizeof(ClientSocket));
        memcpy(MyClientSocket, &param->client_sock, sizeof(ClientSocket));
        read_inheritable_socket(&MyClientSocket->sock, &param->inh_sock);
    }

    // Restore basic process information
    SetDataDir(param->DataDir);
    MyCancelKey = param->MyCancelKey;
    MyPMChildSlot = param->MyPMChildSlot;

    // Restore shared memory segment information
    UsedShmemSegID = param->UsedShmemSegID;
    UsedShmemSegAddr = param->UsedShmemSegAddr;

    // Restore shared memory lock structures
    ShmemLock = param->ShmemLock;
    ShmemBackendArray = param->ShmemBackendArray;
    NamedLWLockTrancheRequests = param->NamedLWLockTrancheRequests;
    NamedLWLockTrancheArray = param->NamedLWLockTrancheArray;
    MainLWLockArray = param->MainLWLockArray;
    ProcStructLock = param->ProcStructLock;

    // Restore process management structures
    ProcGlobal = param->ProcGlobal;
    AuxiliaryProcs = param->AuxiliaryProcs;
    PreparedXactProcs = param->PreparedXactProcs;
    PMSignalState = param->PMSignalState;

    // Restore timing and process information
    PostmasterPid = param->PostmasterPid;
    PgStartTime = param->PgStartTime;
    PgReloadTime = param->PgReloadTime;
    first_syslogger_file_time = param->first_syslogger_file_time;

    // Restore configuration flags
    redirection_done = param->redirection_done;
    IsBinaryUpgrade = param->IsBinaryUpgrade;
    query_id_enabled = param->query_id_enabled;
    max_safe_fds = param->max_safe_fds;
    MaxBackends = param->MaxBackends;

#ifdef WIN32
    // Windows-specific variables
    ShmemProtectiveRegion = param->ShmemProtectiveRegion;
    PostmasterHandle = param->PostmasterHandle;
    pgwin32_initial_signal_pipe = param->initial_signal_pipe;
#else
    // Unix/Linux-specific variables
    memcpy(&postmaster_alive_fds, &param->postmaster_alive_fds, sizeof(postmaster_alive_fds));
#endif

    // Restore logging pipe
    memcpy(&syslogPipe, &param->syslogPipe, sizeof(syslogPipe));

    // Restore path variables
    strlcpy(my_exec_path, param->my_exec_path, MAXPGPATH);
    strlcpy(pkglib_path, param->pkglib_path, MAXPGPATH);

#ifndef WIN32
    // Reserve external file descriptors for postmaster alive pipes
    if (postmaster_alive_fds[0] >= 0)
        ReserveExternalFD();
    if (postmaster_alive_fds[1] >= 0)
        ReserveExternalFD();
#endif
}
```