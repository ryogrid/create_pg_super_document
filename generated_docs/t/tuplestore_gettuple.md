# tuplestore_gettuple

## Location
[src/backend/utils/sort/tuplestore.c:903-1077](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L903-L1077)

## Overview
The core internal function that retrieves the next tuple from a tuplestore in either forward or backward direction, handling both memory-based and file-based storage modes.

## Definition
```c
static void *tuplestore_gettuple(Tuplestorestate *state, bool forward, bool *should_free)
```

## Detailed Description
This static function implements the fundamental tuple retrieval logic for tuplestores, serving as the backend for public functions like tuplestore_gettupleslot. It handles bidirectional navigation across different storage modes and manages complex file positioning for backward scans.

The function operates differently based on the storage state:

**TSS_INMEM (In-Memory Mode):**
- Forward: Returns tuples from the memory array sequentially, marking EOF when exhausted
- Backward: Handles complex positioning logic, accounting for deleted tuples and EOF states
- Returns pointers directly to stored tuples (should_free = false)

**TSS_WRITEFILE (Write-to-File Mode):**
- Transitions from writing to reading mode by saving write position
- Falls through to TSS_READFILE handling after state transition

**TSS_READFILE (Read-from-File Mode):**
- Forward: Uses getlen to read tuple length, then READTUP to retrieve tuple data
- Backward: Implements complex file positioning by reading trailing length words to navigate backwards through variable-length tuples
- Returns allocated tuple copies (should_free = true)

Backward scanning requires special handling because tuples are variable-length and stored sequentially. The function reads trailing length words to determine tuple boundaries and position the file pointer correctly.

## Parameters / Member Variables
- `state`: Pointer to the Tuplestorestate managing the tuplestore
- `forward`: Boolean indicating scan direction (true = forward, false = backward)  
- `should_free`: Output parameter indicating whether caller should pfree the returned tuple

## Dependencies
- Functions called/Symbols referenced:
  - [getlen](../g/getlen.md) (reads tuple length from file)
  - READTUP (macro for reading tuple data)
  - [BufFileTell](../B/BufFileTell.md) (gets current file position)
  - [BufFileSeek](../B/BufFileSeek.md) (seeks to file position)
- Types used:
  - [Tuplestorestate](../T/Tuplestorestate.md)
  - TSReadPointer
- Constants:
  - TSS_INMEM, TSS_WRITEFILE, TSS_READFILE
  - EXEC_FLAG_BACKWARD
- Called from:
  - [tuplestore_gettupleslot](tuplestore_gettupleslot.md)
  - [tuplestore_advance](tuplestore_advance.md)  
  - [tuplestore_skiptuples](tuplestore_skiptuples.md)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Backward scanning is only allowed if randomAccess was set or EXEC_FLAG_BACKWARD was specified
- Memory-based tuples don't need to be freed, but file-based tuples do
- [Complex](../C/Complex.md) backward navigation logic handles variable-length tuple boundaries
- EOF state management differs between forward and backward directions
- File-based backward scans read trailing length words to determine tuple boundaries
- Handles edge cases like seeking beyond file boundaries and deleted tuple ranges
- Returns NULL when no more tuples are available in the requested direction

## Simplified Source

```c
static void *
tuplestore_gettuple(Tuplestorestate *state, bool forward, bool *should_free)
{
    TSReadPointer *readptr = &state->readptrs[state->activeptr];
    unsigned int tuplen;
    void       *tup;

    Assert(forward || (readptr->eflags & EXEC_FLAG_BACKWARD));

    switch (state->status) {
        case TSS_INMEM:
            *should_free = false;
            if (forward) {
                // Forward scan in memory
                if (readptr->eof_reached)
                    return NULL;
                if (readptr->current < state->memtupcount) {
                    return state->memtuples[readptr->current++];
                }
                readptr->eof_reached = true;
                return NULL;
            } else {
                // Backward scan in memory
                if (readptr->eof_reached) {
                    readptr->current = state->memtupcount;
                    readptr->eof_reached = false;
                } else {
                    if (readptr->current <= state->memtupdeleted) {
                        Assert(!state->truncated);
                        return NULL;
                    }
                    readptr->current--;
                }
                if (readptr->current <= state->memtupdeleted) {
                    Assert(!state->truncated);
                    return NULL;
                }
                return state->memtuples[readptr->current - 1];
            }
            break;

        case TSS_WRITEFILE:
            // Skip if already at EOF for forward scan
            if (readptr->eof_reached && forward)
                return NULL;

            // Switch from writing to reading mode
            BufFileTell(state->myfile, &state->writepos_file, &state->writepos_offset);
            if (!readptr->eof_reached) {
                if (BufFileSeek(state->myfile, readptr->file, readptr->offset, SEEK_SET) != 0)
                    ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not seek in tuplestore temporary file")));
            }
            state->status = TSS_READFILE;
            /* FALLTHROUGH */

        case TSS_READFILE:
            *should_free = true;
            if (forward) {
                // Forward scan from file
                if ((tuplen = getlen(state, true)) != 0) {
                    tup = READTUP(state, tuplen);
                    return tup;
                } else {
                    readptr->eof_reached = true;
                    return NULL;
                }
            }

            // Backward scan from file - complex positioning logic
            if (BufFileSeek(state->myfile, 0, -(long) sizeof(unsigned int), SEEK_CUR) != 0) {
                readptr->eof_reached = false;
                Assert(!state->truncated);
                return NULL;
            }
            tuplen = getlen(state, false);

            if (readptr->eof_reached) {
                readptr->eof_reached = false;
            } else {
                // Back up to get previous tuple's ending length
                if (BufFileSeek(state->myfile, 0, -(long) (tuplen + 2 * sizeof(unsigned int)), SEEK_CUR) != 0) {
                    if (BufFileSeek(state->myfile, 0, -(long) (tuplen + sizeof(unsigned int)), SEEK_CUR) != 0)
                        ereport(ERROR, (errcode_for_file_access(),
                               errmsg("could not seek in tuplestore temporary file")));
                    Assert(!state->truncated);
                    return NULL;
                }
                tuplen = getlen(state, false);
            }

            // Position to read the tuple
            if (BufFileSeek(state->myfile, 0, -(long) tuplen, SEEK_CUR) != 0)
                ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not seek in tuplestore temporary file")));
            tup = READTUP(state, tuplen);
            return tup;

        default:
            elog(ERROR, "invalid tuplestore state");
            return NULL;
    }
}
```