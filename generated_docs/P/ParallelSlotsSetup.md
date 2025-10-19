# ParallelSlotsSetup

## Location
[src/fe_utils/parallel_slot.c:428-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/parallel_slot.c#L428-L459)

## Overview
ParallelSlotsSetup is a public function that creates and initializes a parallel slot array for managing multiple database connections, but defers actual database connections until slots are requested.

## Definition

```c
ParallelSlotArray *
ParallelSlotsSetup(int numslots, ConnParams *cparams, const char *progname,
				   bool echo, const char *initcmd)
```
## Detailed Description
This function creates a new ParallelSlotArray structure and initializes it with the specified parameters. It allocates memory for the slot array structure plus space for the specified number of parallel slots. The function stores all connection parameters, program name, echo setting, and initialization command for later use when connections are actually established. All slots are initialized in an idle state with no active connections. The connection parameters and other strings must remain valid throughout the lifetime of the returned array since they are stored by reference.

## Parameters / Member Variables
- `numslots`: Number of parallel slots to create (must be > 0)
- `*cparams`: Connection parameters to use for database connections (must not be NULL)
- `*progname`: Program name for error reporting and logging (must not be NULL)
- `echo`: Whether to echo executed commands
- `*initcmd`: Optional initialization command to execute on new connections; can be NULL
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (allocates zero-initialized memory)
  - [ParallelSlotArray](ParallelSlotArray.md) (structure type)
  - [ParallelSlot](ParallelSlot.md) (slot structure type)
  - [ConnParams](../C/ConnParams.md) (connection parameters type)
- Called from (representative examples):
  - [main](../m/main.md) (pg_amcheck)
  - [reindex_one_database](../r/reindex_one_database.md) (reindexdb)
  - [vacuum_one_database](../v/vacuum_one_database.md) (vacuumdb)
  - [ParallelSlotClearHandler](ParallelSlotClearHandler.md)

## Notes and Other Information
- Returns a pointer to the newly allocated ParallelSlotArray structure
- Uses flexible array member technique to allocate slots in single memory block  
- All slots start in idle state (inUse = false, connection = NULL)
- Connection establishment is deferred until slots are actually requested via ParallelSlotsGetIdle
- Caller is responsible for ensuring parameter lifetime validity
- Memory is allocated using palloc0 for zero-initialization
- Function is part of the public API for PostgreSQL parallel processing utilities
- Essential first step in setting up parallel database operations

## Simplified Source

```c
ParallelSlotArray *
ParallelSlotsSetup(int numslots, ConnParams *cparams, const char *progname,
                   bool echo, const char *initcmd)
{
    ParallelSlotArray *sa;

    Assert(numslots > 0);
    Assert(cparams != NULL);
    Assert(progname != NULL);

    // Allocate memory for slot array plus slots
    sa = (ParallelSlotArray *) palloc0(offsetof(ParallelSlotArray, slots) +
                                       numslots * sizeof(ParallelSlot));

    // Store configuration parameters
    sa->numslots = numslots;
    sa->cparams = cparams;
    sa->progname = progname;
    sa->echo = echo;
    sa->initcmd = initcmd;

    return sa;
}
```