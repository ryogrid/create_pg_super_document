# pg_get_multixact_members

## Location
src/backend/access/transam/multixact.c: 3502 - 3508

## Overview
A PostgreSQL system function that returns detailed information about the member transactions and their lock modes for a given MultiXact ID, useful for debugging and monitoring concurrent tuple-level locks.

## Definition
Datum pg_get_multixact_members(PG_FUNCTION_ARGS)

SQL Interface: pg_get_multixact_members(multixid xid) RETURNS SETOF RECORD (xid xid, mode text)

## Detailed Description
This function is a set-returning function (SRF) that provides detailed inspection capabilities for MultiXact IDs. When a tuple is locked by multiple transactions concurrently, PostgreSQL creates a MultiXact ID to represent the collection of locking transactions. This function decomposes a MultiXact ID to reveal:

1. **Individual transaction IDs**: The specific XIDs of all transactions holding locks
2. **Lock modes**: The type of lock each transaction holds (e.g., 'forupd', 'fornokeyupd', 'sh', 'keysh')

The function implements PostgreSQL's SRF (Set Returning Function) protocol, allowing it to return multiple rows from a single function call. It uses a local struct  to maintain state between calls, storing the member array, count, and current iteration position.

The function validates input by checking that the provided MultiXact ID is valid (>= FirstMultiXactId) and uses GetMultiXactIdMembers() to retrieve the actual member information from the MultiXact subsystem.

## Parameters / Member Variables
-  (input): The MultiXact ID to examine (xid type)

### Internal struct :
- : Pointer to array of MultiXactMember structures containing transaction details
- : Total number of member transactions in the MultiXact
- : Current iteration position for returning results

### Return columns:
- : Transaction ID of each member transaction
- : Text representation of the lock mode ('forupd', 'fornokeyupd', 'sh', 'keysh')

## Dependencies
- Functions called/Symbols referenced:
  - GetMultiXactIdMembers (retrieves member transaction information)
  - mxstatus_to_string (converts lock status enum to text)
  - PG_GETARG_TRANSACTIONID (macro to extract XID argument)
  - SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP (SRF protocol macros)
  - SRF_RETURN_NEXT, SRF_RETURN_DONE (SRF return macros)
  - BuildTupleFromCStrings (constructs result tuples)
  - TupleDescGetAttInMetadata (metadata for tuple construction)
- Data types used:
  - MultiXactId (multi-transaction identifier)
  - MultiXactMember (structure containing xid and status)
  - FuncCallContext (SRF context structure)
  - HeapTuple (result tuple type)
- Called from:
  - SQL queries as a system function
  - Database administration and debugging tools

## Notes and Other Information
- This is primarily a diagnostic and administrative function for examining MultiXact internals
- Corresponds to the SQL system function documented in PostgreSQL's function reference
- The function validates that multixid >= FirstMultiXactId, raising an error for invalid IDs
- Uses PostgreSQL's memory context management to ensure proper cleanup
- Lock mode mappings: 'forupd' (FOR UPDATE), 'fornokeyupd' (FOR NO KEY UPDATE), 'sh' (SHARE), 'keysh' (KEY SHARE)
- Essential for troubleshooting lock contention and understanding concurrent access patterns
- Part of PostgreSQL's system catalog functions for introspection capabilities
- The function does not allow inspection of 'old' values (historical lock information)