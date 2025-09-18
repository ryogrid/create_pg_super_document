# pgstat_tracks_io_op

## Location
src/backend/utils/activity/pgstat_io.c: 424 - 478

## Overview
This function validates whether a specific I/O operation should be tracked for a given combination of backend type, I/O object, I/O context, and I/O operation by implementing comprehensive compatibility rules.

## Definition


## Detailed Description
The `pgstat_tracks_io_op` function serves as the final validation layer in PostgreSQL's I/O statistics tracking system. It validates that a specific I/O operation (IOOp) is valid for the given combination of backend type, I/O object, and I/O context. The function implements several categories of validation rules:

1. **Object/Context Validation**: Delegates to pgstat_tracks_io_object() for initial validation
2. **Backend-specific Operation Restrictions**: Certain backend types don't perform specific operations (e.g., background writer doesn't read)
3. **Object-specific Operation Rules**: Some operations don't apply to certain objects (e.g., temporary tables don't need fsync)
4. **Context-specific Operation Rules**: Some operations are only valid in specific contexts (e.g., IOOP_REUSE only with BufferAccessStrategy)

The function ensures that only meaningful and valid I/O operation combinations are tracked in pg_stat_io.

## Parameters / Member Variables
- `bktype`: The backend type performing the I/O operation
- `io_object`: The type of object being operated on (e.g., IOOBJECT_TEMP_RELATION)
- `io_context`: The context in which the I/O occurs (e.g., IOCONTEXT_NORMAL, IOCONTEXT_BULKREAD)
- `io_op`: The specific I/O operation being performed (e.g., IOOP_READ, IOOP_WRITE, IOOP_FSYNC)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_tracks_io_object](pgstat_tracks_io_object.md)() (validate object/context compatibility)
  - [BackendType](../B/BackendType.md), IOObject, IOContext, IOOp (enum types)
  - B_BG_WRITER, B_CHECKPOINTER, B_AUTOVAC_LAUNCHER (backend type constants)
  - IOOP_READ, IOOP_EVICT, IOOP_HIT, IOOP_EXTEND, IOOP_FSYNC, IOOP_WRITEBACK, IOOP_REUSE (operation constants)
  - IOOBJECT_TEMP_RELATION (object type constant)
  - IOCONTEXT_BULKREAD, IOCONTEXT_BULKWRITE, IOCONTEXT_VACUUM (context constants)
- Called from (representative examples):
  - [pgstat_bktype_io_stats_valid](pgstat_bktype_io_stats_valid.md)() (validate statistics entries)
  - [pgstat_count_io_op_n](pgstat_count_io_op_n.md)() (before counting I/O operations)
  - [pg_stat_get_io](pg_stat_get_io.md)() (filter valid operations when retrieving statistics)

## Notes and Other Information
- Background writer and checkpointer processes don't perform read, evict, or hit operations
- Background writer, checkpointer, and autovacuum launcher don't extend files
- Temporary relations don't require fsync or writeback operations since they're not logged
- IOOP_EXTEND is not valid in IOCONTEXT_BULKREAD context
- IOOP_REUSE is only meaningful when BufferAccessStrategy is in use (bulk contexts)
- IOOP_FSYNC in strategy contexts is counted in IOCONTEXT_NORMAL instead (see register_dirty_segment())
- Currently no cases exist where an operation is invalid for a backend type only within certain contexts/objects
- Part of a hierarchical validation system: bktype → object/context → operation
- Located in src/backend/utils/activity/pgstat_io.c:424-478