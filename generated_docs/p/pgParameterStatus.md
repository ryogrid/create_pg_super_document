# pgParameterStatus

## Location
src/interfaces/libpq/libpq-int.h: 275 - 281

## Overview
pgParameterStatus is a linked list node structure that stores server parameter name-value pairs received from PostgreSQL during connection establishment and runtime parameter updates.

## Definition
```c
typedef struct pgParameterStatus
{
    struct pgParameterStatus *next; /* list link */
    char       *name;               /* parameter name */
    char       *value;              /* parameter value */
    /* Note: name and value are stored in same malloc block as struct is */
} pgParameterStatus;
```

## Detailed Description
The pgParameterStatus structure implements a singly-linked list to maintain PostgreSQL server configuration parameters that have been communicated to the client. When the server sends ParameterStatus messages (typically during connection startup or when SET commands change session parameters), libpq stores these parameter name-value pairs in a linked list rooted at conn->pstatus. This allows client applications to query current server parameter values using PQparameterStatus(). The structure is memory-efficient as the name and value strings are allocated in the same memory block as the structure itself.

## Parameters / Member Variables
- `next`: Pointer to the next pgParameterStatus node in the linked list, NULL for the last node
- `name`: Pointer to null-terminated string containing the parameter name (e.g., "server_version", "client_encoding")
- `value`: Pointer to null-terminated string containing the current parameter value
- Memory layout note: name and value strings are stored in the same malloc block as the structure

## Dependencies
- Functions called/Symbols referenced:
  - Self-references via next pointer for linked list structure
- Used by:
  - pqDropServerData (in fe-connect.c:587, 604) - for cleanup
  - PQparameterStatus (in fe-connect.c:7126) - for parameter lookup
  - pqSaveParameterStatus (in fe-exec.c:1083, 1084, 1107, 1113) - for storing parameters
  - pg_conn structure (in libpq-int.h:511) - as pstatus field

## Notes and Other Information
- Used to implement the ParameterStatus protocol message handling in libpq
- The linked list is traversed by PQparameterStatus() to find specific parameter values
- Memory is allocated efficiently with name/value strings in the same block as the structure
- Common parameters stored include server_version, server_encoding, client_encoding, application_name, etc.
- The list is updated when the server sends ParameterStatus messages during connection or after SET commands