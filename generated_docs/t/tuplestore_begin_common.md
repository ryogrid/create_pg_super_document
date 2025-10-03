# tuplestore_begin_common

## Location
[src/backend/utils/sort/tuplestore.c:253-317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L253-L317)

## Overview
Internal common initialization function for creating a new tuplestore operation, setting up the basic data structures and memory management for tuple storage.

## Definition

```c
static Tuplestorestate *
tuplestore_begin_common(int eflags, bool interXact, int maxKBytes)
```
## Detailed Description
This is the core initialization function used by all tuplestore_begin_xxx variants to create and configure a new Tuplestorestate. It allocates and initializes the main tuplestore data structure with default values, sets up memory management limits, and creates the initial read pointer array. The function establishes the tuplestore in TSS_INMEM status, meaning tuples will initially be stored in memory before potentially being spilled to disk when memory limits are exceeded.

The function calculates an appropriate initial size for the memory tuple array based on ALLOCSET_SEPARATE_THRESHOLD to ensure efficient memory allocation patterns. It also sets up a single default read pointer at position 0.

## Parameters / Member Variables
- `eflags`: Execution flags indicating the capabilities required (e.g., backward scan support)
- `interXact`: Boolean flag indicating whether the tuplestore should survive transaction boundaries
- `maxKBytes`: Maximum memory allowed for the tuplestore in kilobytes before spilling to disk
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation)
  - [palloc](../p/palloc.md) (memory allocation)
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md) (memory tracking)
  - USEMEM (memory accounting macro)
  - Max (macro for maximum value)
- Data structures used:
  - [Tuplestorestate](../T/Tuplestorestate.md) (main tuplestore state structure)
  - TSReadPointer (read pointer structure)
  - TSS_INMEM (tuplestore status enum value)
  - ALLOCSET_SEPARATE_THRESHOLD (memory allocation constant)
- Called from:
  - [tuplestore_begin_heap](tuplestore_begin_heap.md) (heap tuple variant initialization)

## Notes and Other Information
- This is a static function, only accessible within tuplestore.c
- The initial memtuples array size is carefully calculated to work well with PostgreSQL's memory allocator
- Sets up exactly one read pointer initially, but the array can grow to accommodate multiple read pointers
- Memory accounting begins immediately with USEMEM tracking the initial memtuples allocation
- The backward scan capability and function pointers for tuple operations are not set here - they are configured by the calling tuplestore_begin_xxx functions

## Simplified Source

```c
static Tuplestorestate *tuplestore_begin_common(int eflags, bool interXact, int maxKBytes)
{
    Tuplestorestate *state;

    // Allocate and zero-initialize the main tuplestore state
    state = (Tuplestorestate *) palloc0(sizeof(Tuplestorestate));

    // Set basic configuration
    state->status = TSS_INMEM;       // Start in memory-only mode
    state->eflags = eflags;          // Execution flags (e.g., backward scan)
    state->interXact = interXact;    // Survive transaction boundaries?
    state->truncated = false;        // Not truncated initially

    // Set up memory limits
    state->allowedMem = maxKBytes * 1024L;
    state->availMem = state->allowedMem;
    state->myfile = NULL;            // No file initially

    // Remember current contexts
    state->context = CurrentMemoryContext;
    state->resowner = CurrentResourceOwner;

    // Initialize counters
    state->memtupdeleted = 0;
    state->memtupcount = 0;
    state->tuples = 0;

    // Calculate initial array size (must be > ALLOCSET_SEPARATE_THRESHOLD)
    state->memtupsize = Max(16384 / sizeof(void *),
                           ALLOCSET_SEPARATE_THRESHOLD / sizeof(void *) + 1);

    // Allocate the tuple pointer array
    state->growmemtuples = true;
    state->memtuples = (void **) palloc(state->memtupsize * sizeof(void *));
    USEMEM(state, GetMemoryChunkSpace(state->memtuples));

    // Initialize single read pointer
    state->activeptr = 0;
    state->readptrcount = 1;
    state->readptrsize = 8;  // Arbitrary initial size
    state->readptrs = (TSReadPointer *) palloc(state->readptrsize * sizeof(TSReadPointer));

    // Set up first read pointer
    state->readptrs[0].eflags = eflags;
    state->readptrs[0].eof_reached = false;
    state->readptrs[0].current = 0;

    return state;
}
```