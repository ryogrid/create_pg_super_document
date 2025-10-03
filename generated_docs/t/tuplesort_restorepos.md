# tuplesort_restorepos

## Location
[src/backend/utils/sort/tuplesort.c:2504-2536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2504-L2536)

## Overview
Restores the current position in a tuplesort to a previously saved position, enabling backward navigation within sorted results when random access is enabled.

## Definition
```c
void tuplesort_restorepos(Tuplesortstate *state)
```

## Detailed Description
This function restores the current read position in a tuplesort operation to the position that was previously saved using `tuplesort_markpos`. It is part of PostgreSQL's mark/restore functionality that allows for backward navigation within sorted results. The function only works when the TUPLESORT_RANDOMACCESS option is enabled.

The restoration behavior depends on the current status of the tuplesort:
- **TSS_SORTEDINMEM**: Restores the in-memory array position and EOF state directly from saved values
- **TSS_SORTEDONTAPE**: Uses LogicalTapeSeek to restore the tape position to the saved block and offset, then restores EOF state

The function operates within the sort's memory context to ensure proper memory management during the restoration process.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing the sort operation state and saved position information

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [LogicalTapeSeek](../L/LogicalTapeSeek.md)
  - elog
- Constants referenced:
  - TUPLESORT_RANDOMACCESS
  - TSS_SORTEDINMEM
  - TSS_SORTEDONTAPE
- Called from (representative examples):
  - [ExecSortRestrPos](../E/ExecSortRestrPos.md) (in nodeSort.c)

## Notes and Other Information
- Requires TUPLESORT_RANDOMACCESS option to be set during tuplesort initialization
- Must be preceded by a call to `tuplesort_markpos` to save a position
- The function switches to the sort's memory context before performing operations to ensure proper memory allocation
- Will throw an ERROR if called with an invalid tuplesort state
- Part of the mark/restore API that enables features like SCROLL cursors in PostgreSQL

## Simplified Source

```c
void tuplesort_restorepos(Tuplesortstate *state) {
    // Switch to sort context for consistent memory management
    MemoryContext oldcontext = MemoryContextSwitchTo(state->base.sortcontext);

    // Verify random access capability is enabled
    Assert(state->base.sortopt & TUPLESORT_RANDOMACCESS);

    // Restore position based on storage type
    switch (state->status) {
        case TSS_SORTEDINMEM:
            // For in-memory sorts: restore array position and EOF state
            state->current = state->markpos_offset;
            state->eof_reached = state->markpos_eof;
            break;

        case TSS_SORTEDONTAPE:
            // For tape-based sorts: seek to saved tape position
            LogicalTapeSeek(state->result_tape,
                           state->markpos_block,
                           state->markpos_offset);
            state->eof_reached = state->markpos_eof;
            break;

        default:
            elog(ERROR, "invalid tuplesort state");
            break;
    }

    // Restore previous memory context
    MemoryContextSwitchTo(oldcontext);
}
```