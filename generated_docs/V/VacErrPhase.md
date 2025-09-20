# VacErrPhase

## Location
[src/backend/access/heap/vacuumlazy.c:134-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L134-L135)

## Overview
VacErrPhase is an enumerated type that defines different phases of the vacuum operation during which error context information is reported in PostgreSQL's lazy vacuum implementation.

## Definition

```c
typedef enum
{
	VACUUM_ERRCB_PHASE_UNKNOWN,
	VACUUM_ERRCB_PHASE_SCAN_HEAP,
	VACUUM_ERRCB_PHASE_VACUUM_INDEX,
	VACUUM_ERRCB_PHASE_VACUUM_HEAP,
	VACUUM_ERRCB_PHASE_INDEX_CLEANUP,
	VACUUM_ERRCB_PHASE_TRUNCATE,
} VacErrPhase;
```
## Detailed Description
VacErrPhase is used within PostgreSQL's lazy vacuum implementation to track the current phase of vacuum operation for error reporting purposes. This enumeration helps provide meaningful error context when issues occur during different stages of the vacuum process. The enum is defined in src/backend/access/heap/vacuumlazy.c and is used to maintain state information about which specific phase of vacuum is currently being executed.

The enum serves as a mechanism to provide better error diagnostics by identifying exactly which phase of the vacuum operation was in progress when an error occurred, making debugging and troubleshooting more effective.

## Parameters / Member Variables
- : Default/unknown phase state
- : Phase when scanning heap pages
- : Phase when vacuuming index entries  
- : Phase when vacuuming heap tuples
- : Phase when performing index cleanup operations
- : Phase when truncating relation pages

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Used by:
  - [LVRelState](../L/LVRelState.md) (as phase member at src/backend/access/heap/vacuumlazy.c:174)
  - [LVSavedErrInfo](../L/LVSavedErrInfo.md) (as phase member at src/backend/access/heap/vacuumlazy.c:226)

## Notes and Other Information
- This enum is specifically designed for error callback phases during vacuum operations
- It provides granular phase tracking for vacuum operations to improve error reporting and debugging
- Used in conjunction with error context structures to maintain state information during vacuum processing
- Part of PostgreSQL's lazy vacuum implementation which is the default vacuum strategy for most table maintenance operations