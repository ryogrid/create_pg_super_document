# do_setval

## Location
src/backend/commands/sequence.c: 945 - 1048

## Overview
Internal procedure that implements the core functionality for both 2-argument and 3-argument forms of SETVAL, allowing manual setting of sequence values and state.

## Definition


## Detailed Description
The do_setval function is the main internal implementation for PostgreSQL's setval() functionality. It allows setting a sequence's current value and optionally its 'is_called' flag. The function supports both the 2-argument form (which assumes is_called=true) and the 3-argument form (which allows explicit control over the is_called flag).

The 3-argument form is primarily designed for pg_dump operations during data restoration, allowing exact sequence state recovery. When iscalled=false, the next call to nextval() will return the set value; when iscalled=true, nextval() will return the set value plus increment.

The function performs comprehensive validation including permission checks (ACL_UPDATE required), bounds checking against sequence min/max values, and prevents execution in read-only transactions (except for temporary sequences) and parallel mode. It also handles proper WAL logging for crash recovery.

## Parameters / Member Variables
-  (Oid): Object identifier of the sequence relation to modify
-  (int64): The value to set as the sequence's current position  
-  (bool): Whether the sequence should be marked as having been called (affects next nextval() behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [init_sequence](../i/init_sequence.md): Initialize and lock the sequence relation
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md): Check UPDATE permissions on the sequence
  - [SearchSysCache1](../S/SearchSysCache1.md)/ReleaseSysCache: Look up sequence metadata
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md): Block execution in read-only transactions
  - [PreventCommandIfParallelMode](../P/PreventCommandIfParallelMode.md): Block execution in parallel mode
  - [read_seq_tuple](../r/read_seq_tuple.md): Read the sequence data tuple from storage
  - RelationNeedsWAL: Check if WAL logging is required
  - [GetTopTransactionId](../G/GetTopTransactionId.md): Ensure transaction ID for WAL
  - XLog functions: Write-ahead logging for crash recovery
  - [sequence_close](../s/sequence_close.md): Close and unlock the sequence relation
- Called from (representative examples):
  - [setval_oid](../s/setval_oid.md): 2-argument setval wrapper
  - [setval3_oid](../s/setval3_oid.md): 3-argument setval wrapper

## Notes and Other Information
- Static function, not directly callable from SQL (accessed via setval_oid/setval3_oid wrappers)
- Requires UPDATE permission on the sequence, not just USAGE
- Validates that the new value is within the sequence's defined min/max bounds
- Updates both in-memory cache (SeqTable) and persistent storage
- Handles proper WAL logging for crash recovery and replication
- The 3-argument form with iscalled=false is primarily for pg_dump restoration
- Prevents execution in parallel mode due to backend-local sequence cache limitations
- Part of PostgreSQL's sequence management system in src/backend/commands/sequence.c:945