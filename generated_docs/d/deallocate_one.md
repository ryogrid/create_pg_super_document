# deallocate_one

## Location
src/interfaces/ecpg/ecpglib/prepare.c: 260 - 314

## Overview
A static function that deallocates a single prepared statement from the ECPG (Embedded SQL in C for PostgreSQL) library, removing both the backend prepared statement and freeing associated memory resources.

## Definition


## Detailed Description
The  function performs the complete deallocation of a prepared statement in the ECPG library. It operates in two phases: first sending a DEALLOCATE command to the PostgreSQL backend to remove the prepared statement from the server, and then freeing all client-side memory resources associated with the statement. The function handles different compatibility modes and manages the linked list structure of prepared statements by properly updating pointers when removing a statement from the chain.

## Parameters / Member Variables
- : Line number in the source code where the deallocation is requested (for error reporting)
- : Compatibility mode enumeration that affects error handling behavior
- : Pointer to the database connection structure
- : Pointer to the previous prepared statement in the linked list (NULL if this is the first statement)
- : Pointer to the prepared statement to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_log (logging functionality)
  - ecpg_alloc (memory allocation)
  - PQexec (PostgreSQL command execution)
  - ecpg_free (memory deallocation)
  - ecpg_check_PQresult (result validation)
  - INFORMIX_MODE (compatibility mode check)
  - ecpg_raise (error reporting)
- Called from (representative examples):
  - ECPGdeallocate
  - ecpg_deallocate_all_conn
  - ecpg_freeStmtCacheEntry

## Notes and Other Information
- The function ignores backend deallocation errors in INFORMIX compatibility mode to maintain compatibility with legacy behavior
- Memory cleanup is performed regardless of backend deallocation success to prevent memory leaks
- The function properly maintains the linked list integrity by updating the previous statement's next pointer or the connection's prep_stmts head pointer
- Error handling includes raising ECPG_INVALID_STMT errors for invalid statement names in non-INFORMIX modes