# pgstat_tracks_io_op

## Location
[src/backend/utils/activity/pgstat_io.c:424-478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L424-L478)

## Overview
This function validates whether a specific I/O operation should be tracked for a given combination of backend type, I/O object, I/O context, and I/O operation by implementing comprehensive compatibility rules.

## Definition

```c
bool
pgstat_tracks_io_op(BackendType bktype, IOObject io_object,
					IOContext io_context, IOOp io_op)
```
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

## Simplified Source

```c
// Simplified version of pgstat_tracks_io_op
bool
pgstat_tracks_io_op(BackendType bktype, IOObject io_object,
                    IOContext io_context, IOOp io_op)
{
    // Step 1: Check if this object/context combination tracks stats at all
    if (!pgstat_tracks_io_object(bktype, io_object, io_context))
        return false;

    // Step 2: Backend-specific operation restrictions
    // Background writer and checkpointer don't do reads, evicts, or hits
    if ((bktype == B_BG_WRITER || bktype == B_CHECKPOINTER) &&
        (io_op == IOOP_READ || io_op == IOOP_EVICT || io_op == IOOP_HIT))
        return false;

    // These backend types don't extend files
    if ((bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER ||
         bktype == B_CHECKPOINTER) && io_op == IOOP_EXTEND)
        return false;

    // Step 3: Object-specific operation rules
    // Temporary tables don't need fsync or writeback (not logged)
    if (io_object == IOOBJECT_TEMP_RELATION &&
        (io_op == IOOP_FSYNC || io_op == IOOP_WRITEBACK))
        return false;

    // Step 4: Context-specific operation rules
    // Can't extend files during bulk reads
    if (io_context == IOCONTEXT_BULKREAD && io_op == IOOP_EXTEND)
        return false;

    // Check if we're in a strategy context (bulk operations)
    bool strategy_context = (io_context == IOCONTEXT_BULKREAD ||
                           io_context == IOCONTEXT_BULKWRITE ||
                           io_context == IOCONTEXT_VACUUM);

    // REUSE operations only happen with buffer access strategies
    if (!strategy_context && io_op == IOOP_REUSE)
        return false;

    // FSYNC in strategy contexts gets counted in NORMAL context instead
    if (strategy_context && io_op == IOOP_FSYNC)
        return false;

    return true;
}
```

Key simplifications made:
- Added descriptive step-by-step comments explaining the validation layers
- Clarified the purpose of each conditional check with inline comments
- Grouped related validation rules together logically
- Renamed `strategy_io_context` to `strategy_context` for clarity
- Added explanatory comments for business logic (e.g., "not logged" for temp tables)
- Preserved all essential validation logic while making the flow more readable
- Maintained the exact same conditional logic and return behavior