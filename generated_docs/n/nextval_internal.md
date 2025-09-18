# nextval_internal

## Location
src/backend/commands/sequence.c: 623 - 865

## Overview
The core implementation function that generates the next value from a PostgreSQL sequence, handling caching, WAL logging, and all sequence state management.

## Definition
```c
int64 nextval_internal(Oid relid, bool check_permissions)
```

## Detailed Description
This function is the heart of PostgreSQL's sequence value generation system. It implements sophisticated sequence value caching, WAL logging optimizations, and proper transaction handling for sequences. The function handles both ascending and descending sequences, supports cycling, and implements an efficient caching mechanism to reduce WAL overhead.

Key features include:
- Multi-value caching to reduce WAL logging frequency
- Support for both cyclic and non-cyclic sequences  
- Proper handling of sequence limits (MAXVALUE/MINVALUE)
- WAL logging with checkpoint awareness
- Permission checking and transaction safety
- Protection against parallel execution and read-only transactions

The function uses a two-phase approach: first it checks if cached values are available, and if not, it fetches a new batch of values from the sequence relation, potentially logging some of them to WAL for crash recovery.

## Parameters / Member Variables
- `relid`: The OID of the sequence relation to operate on
- `check_permissions`: Whether to verify ACL_USAGE and ACL_UPDATE permissions for the sequence

## Dependencies
- Functions called/Symbols referenced:
  - [init_sequence](../i/init_sequence.md) (sequence initialization and locking)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md) (permission verification)
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)/PreventCommandIfParallelMode (safety checks)
  - [SearchSysCache1](../S/SearchSysCache1.md) (sequence metadata lookup)
  - [read_seq_tuple](../r/read_seq_tuple.md) (sequence tuple reading)
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md)/PageGetLSN (WAL checkpoint handling)
  - RelationNeedsWAL/GetTopTransactionId (WAL logging setup)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterBuffer/XLogInsert (WAL logging)
  - MarkBufferDirty (buffer management)
  - [sequence_close](../s/sequence_close.md) (resource cleanup)
- Called from (representative examples):
  - [nextval](nextval.md) (text-based sequence interface)
  - [nextval_oid](nextval_oid.md) (OID-based sequence interface) 
  - [ExecEvalNextValueExpr](../E/ExecEvalNextValueExpr.md) (executor for nextval expressions)

## Notes and Other Information
- Implements sequence value caching to reduce WAL log volume (SEQ_LOG_VALS optimization)
- Uses critical sections around buffer modifications to ensure consistency
- Handles checkpoint boundaries by checking page LSN against redo pointer
- Supports both temp sequences (no WAL) and persistent sequences (with WAL)
- Maintains backend-local sequence cache in SeqTable for performance
- Prevents use in parallel query execution due to cache sharing limitations
- Returns int64 values supporting PostgreSQL's full bigint sequence range
- Central to PostgreSQL's sequence performance through intelligent caching and WAL management