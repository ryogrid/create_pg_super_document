# freePGconn

## Location
[src/interfaces/libpq/fe-connect.c:4634-4715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L4634-L4715)

## Overview
Deallocates and cleans up a PGconn data structure, freeing all associated memory and resources held for the lifetime of the connection object.

## Definition

```c
static void
freePGconn(PGconn *conn)
```
## Detailed Description
The  function is responsible for the complete deallocation of a PGconn structure and all its associated resources. This function is designed to complement  by handling cleanup of data that persists for the entire lifetime of the connection object, rather than transient state that gets reset during connection reuse.

The function performs comprehensive cleanup in a specific order:
1. Notifies registered event procedures of connection destruction
2. Releases network address information and host connection data  
3. Frees all string-based connection parameters and configuration
4. Securely clears sensitive data like passwords using 
5. Deallocates buffers and internal data structures
6. Finally frees the main PGconn structure itself

Special attention is paid to security by explicitly zeroing password fields before freeing them to prevent sensitive data from remaining in memory.

## Parameters / Member Variables
- : A pointer to the PGconn structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  -  (event structure)
  -  (event type constant)
  -  (address info cleanup)
  -  (host data cleanup)
  -  (secure memory clearing)
  -  (buffer termination)
  -                total        used        free      shared  buff/cache   available
Mem:        32819372     7074928    24496036       13740     1248408    25357716
Swap:        8388608           0     8388608 (memory deallocation)

- Called from (representative examples):
  - 
  -  (on allocation failure)
  - 

## Notes and Other Information
- Should not overlap functionality with  - handles lifetime data only
- Processes all registered event handlers before cleanup, allowing them to clean up their state
- Uses  for password fields (, ) to securely clear sensitive data
- Does not close or free  as it's not owned by the connection
- Frees dozens of string parameters including SSL settings, authentication data, and connection options
- Terminates PQExpBuffer objects for error messages and work buffers before freeing
- The function assumes the connection has already been properly closed before being called
- Memory cleanup follows a logical order from higher-level constructs to basic allocations
- All dynamically allocated string fields are freed, even if they might be NULL (free() handles NULL gracefully)