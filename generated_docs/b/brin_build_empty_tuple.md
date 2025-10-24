# brin_build_empty_tuple

## Location
[src/backend/access/brin/brin.c:2943-2979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L2943-L2979)

## Overview
This function creates and manages a reusable empty BRIN tuple representing an empty page range, optimizing memory usage by initializing it only once and updating just the block number for subsequent uses.

## Definition
```c
static void brin_build_empty_tuple(BrinBuildState *state, BlockNumber blkno)
```

## Detailed Description
This function implements a lazy initialization pattern for empty BRIN tuples. When called for the first time, it creates a new BRIN memory tuple, forms it into a proper BRIN tuple, and stores it in the build state's memory context to ensure it persists for the entire index build process. On subsequent calls, it simply updates the block number field (bt_blkno) of the existing empty tuple, avoiding the overhead of creating new tuples for each empty range. This optimization is particularly beneficial when dealing with sparse tables that have many empty page ranges.

## Parameters / Member Variables
- `state`: The BRIN build state containing the tuple descriptor and memory context
- `blkno`: The block number where this empty range starts

## Dependencies
- Functions called/Symbols referenced:
  - [brin_new_memtuple](brin_new_memtuple.md): Creates a new BRIN memory tuple using the tuple descriptor
  - [brin_form_tuple](brin_form_tuple.md): Converts a memory tuple into a proper BRIN tuple format
  - [BrinBuildState](../B/BrinBuildState.md): Structure containing build state and memory context
  - [BrinMemTuple](../B/BrinMemTuple.md): In-memory representation of a BRIN tuple

- Called from (representative examples):
  - [brin_fill_empty_ranges](brin_fill_empty_ranges.md): Uses this function to create empty tuples for gaps in the index

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- Uses lazy initialization pattern - empty tuple is created only when first needed
- Memory allocation occurs in the build state's context (bs_context) to ensure proper lifetime management
- All empty tuples are identical except for the bt_blkno field
- The optimization reduces memory allocation overhead when building indexes on sparse tables
- The empty tuple length is stored in bs_emptyTupleLen for reuse
- The function switches memory contexts to ensure the empty tuple persists throughout the index build
- Subsequent calls are very efficient, only requiring a single field update

## Simplified Source

```c
static void
brin_build_empty_tuple(BrinBuildState *state, BlockNumber blkno)
{
    // Lazy initialization: create empty tuple only once
    if (state->bs_emptyTuple == NULL)
    {
        MemoryContext oldcxt;
        BrinMemTuple *dtuple;

        // Create new empty memory tuple
        dtuple = brin_new_memtuple(state->bs_bdesc);

        // Switch to persistent memory context
        oldcxt = MemoryContextSwitchTo(state->bs_context);

        // Form persistent empty tuple and store length
        state->bs_emptyTuple = brin_form_tuple(state->bs_bdesc, blkno, dtuple,
                                               &state->bs_emptyTupleLen);

        MemoryContextSwitchTo(oldcxt);
    }
    else
    {
        // Reuse existing empty tuple, just update block number
        state->bs_emptyTuple->bt_blkno = blkno;
    }
}
```