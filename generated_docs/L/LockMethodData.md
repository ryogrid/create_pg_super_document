# LockMethodData

## Location
src/include/storage/lock.h: 108 - 114

## Overview
LockMethodData defines the locking semantics and configuration for a specific lock method in PostgreSQL. It specifies lock modes, their conflicts, and debugging information for a particular locking subsystem.

## Definition

```c
typedef struct LockMethodData
{
	int			numLockModes;
	const LOCKMASK *conflictTab;
	const char *const *lockModeNames;
	const bool *trace_flag;
} LockMethodData;
```
## Detailed Description
LockMethodData is a fundamental data structure that encapsulates the complete locking semantics for a lock method. PostgreSQL uses different lock methods for different types of resources (e.g., tables, pages, tuples), and each method defines its own set of lock modes and conflict rules.

The structure is designed to be immutable - all data is constant and kept in static tables. This design ensures thread safety and allows for efficient lock conflict checking through bitmask operations.

The conflict checking is performed using the conflictTab array, where each lock mode has an associated bitmask indicating which other lock modes it conflicts with. This allows for very fast conflict detection using bitwise operations.

## Parameters / Member Variables
- : Number of distinct lock modes defined for this lock method (must be less than MAX_LOCKMODES)
- : Array of bitmasks defining lock mode conflicts; conflictTab[i] has the j-th bit set if modes i and j conflict (index 0 is unused, modes numbered 1..numLockModes)
- : Array of string names for each lock mode, used for debugging and logging
- : Pointer to GUC trace flag that controls debug output for this lock method

## Dependencies
- Functions called/Symbols referenced:
  - LOCKMASK
- Called from (representative examples):
  - NLOCKENTS (lock.c)
  - LockMethod

## Notes and Other Information
- All data in this structure is constant and immutable for thread safety
- Lock modes are numbered starting from 1, with index 0 being unused in conflictTab
- The conflictTab uses bitmask operations for efficient conflict checking
- Different lock methods (table-level, page-level, etc.) have their own LockMethodData instances
- The trace_flag allows runtime control of debugging output per lock method
- This structure is fundamental to PostgreSQL's multi-granularity locking system
- Lock conflict detection is one of the most performance-critical operations in the lock manager