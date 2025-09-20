# BackendStartupData

## Location
[src/include/tcop/backend_startup.h:34-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tcop/backend_startup.h#L34-L37)

## Overview
BackendStartupData is a structure that carries initialization information from the postmaster process to a newly spawned backend process, specifically containing the connection acceptance state.

## Definition

```c
typedef struct BackendStartupData
{
	CAC_state	canAcceptConnections;
} BackendStartupData;
```
## Detailed Description
BackendStartupData serves as a communication mechanism between the postmaster and backend processes during backend initialization. The structure is passed through the startup_data parameter when a new backend process is created. It contains essential state information that determines whether the backend should proceed with accepting and processing the client connection or should reject it with an appropriate error message.

The structure is designed to be simple and compact, containing only the essential information needed for the backend to make connection acceptance decisions. This design reflects PostgreSQL's architecture where the postmaster makes high-level decisions about system state and resource availability, which are then communicated to individual backend processes.

## Parameters / Member Variables
- : A CAC_state enum value that indicates whether the backend should accept new connections. Possible values include:
  - CAC_OK: Normal operation, connections can be accepted
  - CAC_STARTUP: System is starting up, reject connections
  - CAC_SHUTDOWN: System is shutting down, reject connections  
  - CAC_RECOVERY: System is in recovery mode, reject connections
  - CAC_NOTCONSISTENT: Database is not in a consistent state, reject connections
  - CAC_TOOMANY: Too many connections already active, reject connections

## Dependencies
- Functions called/Symbols referenced:
  - CAC_state (enum type)
- Called from (representative examples):
  - [BackendStartup](BackendStartup.md) (in postmaster.c:3549)
  - [BackendMain](BackendMain.md) (in backend_startup.c:59, 61)

## Notes and Other Information
- The structure is passed as raw bytes through the startup_data parameter and is cast back to BackendStartupData* in the backend process
- Size validation is performed with Assert(startup_data_len == sizeof(BackendStartupData)) to ensure data integrity
- The canAcceptConnections value is determined by calling canAcceptConnections(BACKEND_TYPE_NORMAL) in the postmaster process
- If canAcceptConnections is not CAC_OK, the backend is marked as a "dead end" backend that will terminate after sending an error to the client
- This mechanism allows the postmaster to control connection acceptance based on system state without requiring complex inter-process communication