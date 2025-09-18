# parse_snapshot

## Location
src/backend/utils/adt/xid8funcs.c: 265 - 333

## Overview
A static function that parses a string representation of a PostgreSQL snapshot into a pg_snapshot structure.

## Definition
```c
static pg_snapshot *parse_snapshot(const char *str, Node *escontext)
```

## Detailed Description
This function converts a textual snapshot representation (in the format "xmin:xmax:active_xid1,active_xid2,...") into a pg_snapshot structure. It validates the format and ordering requirements, ensuring xmin and xmax are valid, xmin precedes xmax, and all active transaction IDs are properly ordered between xmin and xmax. The function uses helper functions buf_init, buf_add_txid, and buf_finalize to construct the snapshot efficiently, and includes duplicate detection to avoid redundant entries.

## Parameters / Member Variables
- `str`: String representation of the snapshot to parse (format: "xmin:xmax:active_xid1,active_xid2,...")
- `escontext`: Error context node for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdFromU64
  - strtou64
  - FullTransactionIdIsValid
  - FullTransactionIdPrecedes
  - FullTransactionIdFollowsOrEquals
  - FullTransactionIdEquals
  - buf_init
  - buf_add_txid
  - buf_finalize
  - ereturn
- Types referenced:
  - FullTransactionId
  - InvalidFullTransactionId
  - pg_snapshot
  - Node
  - StringInfo
- Called from (representative examples):
  - pg_snapshot_in

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/xid8funcs.c
- The function expects input in the specific format "xmin:xmax:active_xid1,active_xid2,..."
- Active transaction IDs must be sorted in ascending order and fall between xmin and xmax
- Duplicate transaction IDs are automatically filtered out
- Uses soft error handling through the escontext parameter
- Returns NULL on parse error with appropriate error message
- The snapshot string format follows PostgreSQL's internal snapshot representation