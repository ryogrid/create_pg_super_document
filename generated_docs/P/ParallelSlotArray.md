# ParallelSlotArray

## Location
[src/include/fe_utils/parallel_slot.h:36-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/parallel_slot.h#L36-L44)

## Overview
ParallelSlotArray is a structure that manages an array of parallel worker slots for PostgreSQL frontend utilities, providing coordination and management capabilities for parallel operations.

## Definition

```c
typedef struct ParallelSlotArray
{
	int			numslots;
	ConnParams *cparams;
	const char *progname;
	bool		echo;
	const char *initcmd;
	ParallelSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ParallelSlotArray;
```
## Detailed Description
ParallelSlotArray serves as a container and management structure for multiple ParallelSlot instances in PostgreSQL frontend utilities. It encapsulates the configuration and state needed to coordinate parallel worker processes, including connection parameters, program identification, and initialization commands. The structure uses a flexible array member to dynamically allocate the exact number of slots needed for parallel operations.

This structure is commonly used in utilities like pg_amcheck, reindexdb, and vacuumdb to enable parallel processing capabilities, allowing multiple database connections to work concurrently on different tasks.

## Parameters / Member Variables
- : Number of parallel slots allocated in the slots array
- : Connection parameters structure containing database connection information
- : Name of the program using this parallel slot array (for identification/logging purposes)
- : Boolean flag indicating whether to echo commands or SQL statements
- : Initialization command to execute when setting up new worker connections
- : Flexible array of ParallelSlot structures representing individual worker slots

## Dependencies
- Functions called/Symbols referenced:
  - [ConnParams](../C/ConnParams.md)
  - [ParallelSlot](ParallelSlot.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [ParallelSlotsSetup](ParallelSlotsSetup.md)
  - [ParallelSlotsGetIdle](ParallelSlotsGetIdle.md)
  - [ParallelSlotsTerminate](ParallelSlotsTerminate.md)
  - [ParallelSlotsWaitCompletion](ParallelSlotsWaitCompletion.md)

## Notes and Other Information
- This structure is defined in src/include/fe_utils/parallel_slot.h:36-44
- Uses flexible array member for dynamic slot allocation based on the specified number of parallel workers
- Commonly used across multiple PostgreSQL frontend utilities (pg_amcheck, reindexdb, vacuumdb)
- Provides a unified interface for managing parallel database operations in client-side tools
- The echo flag allows utilities to control output verbosity during parallel operations
- Connection parameters are shared across all slots, but each slot maintains its own connection state