# pgstat_tracks_io_object

## Location
src/backend/utils/activity/pgstat_io.c: 359 - 423

## Overview
This function determines whether I/O statistics should be tracked for a specific combination of backend type, I/O object, and I/O context by validating compatibility and filtering out invalid or uninteresting combinations.

## Definition


## Detailed Description
The `pgstat_tracks_io_object` function performs multi-layered validation to determine if I/O statistics should be tracked for a given combination of parameters. It implements several filtering rules:

1. **Backend Type Validation**: First checks if the backend type supports I/O tracking using pgstat_tracks_io_bktype()
2. **Temporary Relations Context**: Validates that temporary relations only operate in IOCONTEXT_NORMAL
3. **Backend-specific Limitations**: Excludes certain backend types from operating on temporary relations based on their architectural constraints
4. **Context-specific Exclusions**: Filters out backend/context combinations that don't occur in practice to simplify the statistics view

The function helps maintain a clean and meaningful pg_stat_io view by excluding combinations that either cannot occur or are not practically useful for monitoring.

## Parameters / Member Variables
- `bktype`: The backend type to check for I/O tracking compatibility
- `io_object`: The type of I/O object (e.g., IOOBJECT_TEMP_RELATION)
- `io_context`: The I/O context (e.g., IOCONTEXT_NORMAL, IOCONTEXT_BULKREAD, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_tracks_io_bktype() (validate backend type eligibility)
  - BackendType, IOObject, IOContext (enum types)
  - IOCONTEXT_NORMAL, IOCONTEXT_BULKREAD, IOCONTEXT_BULKWRITE, IOCONTEXT_VACUUM (context constants)
  - IOOBJECT_TEMP_RELATION (object type constant)
  - B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_STANDALONE_BACKEND, B_STARTUP (backend type constants)
- Called from (representative examples):
  - pgstat_tracks_io_op() (to validate object/context before checking operation tracking)
  - pg_stat_get_io() (to filter valid combinations when retrieving I/O statistics)

## Notes and Other Information
- Returns false for backend types that don't support I/O tracking (delegates to pgstat_tracks_io_bktype)
- Temporary relations can only be accessed in IOCONTEXT_NORMAL context
- Certain backend types (B_AUTOVAC_LAUNCHER, B_BG_WRITER, B_CHECKPOINTER, B_AUTOVAC_WORKER, B_STANDALONE_BACKEND, B_STARTUP) do not operate on temporary relations
- Background workers (B_BG_WORKER) can operate on temporary relations to support extensions
- Checkpointer and background writer processes are excluded from bulk and vacuum contexts
- Autovacuum launcher is excluded from vacuum context, and autovacuum processes are excluded from bulk write context
- Designed to make pg_stat_io view more user-friendly by excluding irrelevant combinations
- Located in src/backend/utils/activity/pgstat_io.c:359-423