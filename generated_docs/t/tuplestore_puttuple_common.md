# tuplestore_puttuple_common

## Location
[src/backend/utils/sort/tuplestore.c:765-902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L765-L902)

## Overview
The core internal function that handles the actual storage of tuples in a tuplestore, managing the transition between memory-based and file-based storage modes.

## Definition
```c
static void tuplestore_puttuple_common(Tuplestorestate *state, void *tuple)
```

## Detailed Description
This static function implements the core logic for storing tuples in a tuplestore, regardless of their original format. It serves as the common backend for all the public tuple insertion functions (tuplestore_puttupleslot, tuplestore_puttuple, tuplestore_putvalues).

The function manages three distinct storage states:

**TSS_INMEM (In-Memory Mode):**
- Stores tuples in a memory array (memtuples)
- Updates read pointers for non-active pointers that were at EOF
- Attempts to grow the memory array when approaching capacity
- Transitions to file-based storage when memory or array slots are exhausted

**TSS_WRITEFILE (Write-to-File Mode):**
- Writes tuples directly to the temporary file using WRITETUP
- Updates read pointer positions using BufFileTell for EOF pointers
- Most efficient mode for sequential writing

**TSS_READFILE (Read-from-File Mode):**
- Handles the transition from reading back to writing
- Saves the current read position and seeks to the write position
- Updates all read pointers appropriately
- Switches state back to TSS_WRITEFILE

The function also manages read pointer behavior according to the API specification: non-active read pointers at EOF are moved to track new tuples, while other pointers remain unchanged.

## Parameters / Member Variables
- `state`: Pointer to the Tuplestorestate structure managing the tuplestore
- `tuple`: Generic pointer to the tuple data to be stored (can be MinimalTuple or HeapTuple)

## Dependencies
- Functions called/Symbols referenced:
  - [grow_memtuples](../g/grow_memtuples.md)
  - LACKMEM (macro)
  - [PrepareTempTablespaces](../P/PrepareTempTablespaces.md)
  - [BufFileCreateTemp](../B/BufFileCreateTemp.md)
  - [dumptuples](../d/dumptuples.md)
  - [BufFileTell](../B/BufFileTell.md)
  - [BufFileSeek](../B/BufFileSeek.md)
  - WRITETUP (macro)
- Types used:
  - [Tuplestorestate](../T/Tuplestorestate.md)
  - TSReadPointer
  - [ResourceOwner](../R/ResourceOwner.md)
- Constants:
  - TSS_INMEM, TSS_WRITEFILE, TSS_READFILE
  - EXEC_FLAG_BACKWARD
- Called from:
  - [tuplestore_puttupleslot](tuplestore_puttupleslot.md)
  - [tuplestore_puttuple](tuplestore_puttuple.md)
  - [tuplestore_putvalues](tuplestore_putvalues.md)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Handles all the complex state management and transitions between storage modes
- The decision to use backward reading support is frozen when transitioning to file-based storage
- Temporary files are created in appropriate temp tablespaces
- Read pointer management follows specific rules to support Material and CTE scan node requirements
- Error handling includes proper resource owner management for temporary files
- The function increments the total tuple count regardless of storage mode

## Simplified Source

```c
static void tuplestore_puttuple_common(Tuplestorestate *state, void *tuple)
{
    TSReadPointer *readptr;
    int i;

    state->tuples++;  // Increment total tuple count

    switch (state->status)
    {
        case TSS_INMEM:
            // Update read pointers at EOF (except active pointer)
            for (i = 0; i < state->readptrcount; i++)
            {
                readptr = &state->readptrs[i];
                if (readptr->eof_reached && i != state->activeptr)
                {
                    readptr->eof_reached = false;
                    readptr->current = state->memtupcount;
                }
            }

            // Try to grow memory array if nearly full
            if (state->memtupcount >= state->memtupsize - 1)
                grow_memtuples(state);

            // Store tuple in memory array
            state->memtuples[state->memtupcount++] = tuple;

            // Stay in memory if we have space
            if (state->memtupcount < state->memtupsize && !LACKMEM(state))
                return;

            // Transition to file-based storage
            PrepareTempTablespaces();
            state->myfile = BufFileCreateTemp(state->interXact);
            state->backward = (state->eflags & EXEC_FLAG_BACKWARD) != 0;
            state->status = TSS_WRITEFILE;
            dumptuples(state);  // Write memory tuples to file
            break;

        case TSS_WRITEFILE:
            // Update EOF read pointers with current file position
            for (i = 0; i < state->readptrcount; i++)
            {
                readptr = &state->readptrs[i];
                if (readptr->eof_reached && i != state->activeptr)
                {
                    readptr->eof_reached = false;
                    BufFileTell(state->myfile, &readptr->file, &readptr->offset);
                }
            }
            WRITETUP(state, tuple);  // Write tuple to file
            break;

        case TSS_READFILE:
            // Switch from reading to writing mode
            if (!state->readptrs[state->activeptr].eof_reached)
                BufFileTell(state->myfile,
                           &state->readptrs[state->activeptr].file,
                           &state->readptrs[state->activeptr].offset);

            // Seek to write position
            BufFileSeek(state->myfile, state->writepos_file, state->writepos_offset, SEEK_SET);
            state->status = TSS_WRITEFILE;

            // Update read pointers
            for (i = 0; i < state->readptrcount; i++)
            {
                readptr = &state->readptrs[i];
                if (readptr->eof_reached && i != state->activeptr)
                {
                    readptr->eof_reached = false;
                    readptr->file = state->writepos_file;
                    readptr->offset = state->writepos_offset;
                }
            }
            WRITETUP(state, tuple);
            break;

        default:
            elog(ERROR, "invalid tuplestore state");
    }
}
```