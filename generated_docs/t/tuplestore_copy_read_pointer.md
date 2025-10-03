# tuplestore_copy_read_pointer

## Location
[src/backend/utils/sort/tuplestore.c:1268-1359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L1268-L1359)

## Overview
Copies the state of one read pointer to another read pointer within a tuplestore, handling different storage states and maintaining file position consistency.

## Definition

```c
void
tuplestore_copy_read_pointer(Tuplestorestate *state,
							 int srcptr, int destptr)
```
## Detailed Description
This function copies all the state information from a source read pointer to a destination read pointer within the same tuplestore. The operation handles different tuplestore states (in-memory, writing to file, reading from file) appropriately. When copying pointers in file-based mode, it must carefully manage file seek positions since the active read pointer's position corresponds to the actual file seek point rather than just the stored variables. The function also recomputes the overall eflags if the destination pointer's flags differ from the source.

## Parameters / Member Variables
- `state`: The tuplestore state containing the read pointers
- `srcptr`: Index of the source read pointer to copy from
- `destptr`: Index of the destination read pointer to copy to

## Dependencies
- Functions called/Symbols referenced:
  - [Tuplestorestate](../T/Tuplestorestate.md)
  - TSReadPointer
  - TSS_INMEM, TSS_WRITEFILE, TSS_READFILE (tuplestore status constants)
  - [BufFileSeek](../B/BufFileSeek.md)
  - [BufFileTell](../B/BufFileTell.md)
- Called from (representative examples):
  - [ExecMaterialMarkPos](../E/ExecMaterialMarkPos.md)
  - [ExecMaterialRestrPos](../E/ExecMaterialRestrPos.md)

## Notes and Other Information
- The function performs validation to ensure both pointer indices are within valid range
- Self-assignment (srcptr == destptr) is treated as a no-op
- When eflags differ between pointers, the function recomputes the overall state eflags by OR-ing all read pointer eflags
- In TSS_READFILE state, special handling is required for the active read pointer since its logical position may differ from the file's actual seek position
- File operations can raise ERROR conditions if seek operations fail

## Simplified Source

```c
void tuplestore_copy_read_pointer(Tuplestorestate *state,
                                  int srcptr, int destptr) {
    TSReadPointer *sptr = &state->readptrs[srcptr];
    TSReadPointer *dptr = &state->readptrs[destptr];

    // Validate pointer indices
    Assert(srcptr >= 0 && srcptr < state->readptrcount);
    Assert(destptr >= 0 && destptr < state->readptrcount);

    // Self-assignment is a no-op
    if (srcptr == destptr) {
        return;
    }

    // Handle potential eflags changes
    if (dptr->eflags != sptr->eflags) {
        // Copy and recompute overall eflags
        *dptr = *sptr;
        int eflags = state->readptrs[0].eflags;
        for (int i = 1; i < state->readptrcount; i++) {
            eflags |= state->readptrs[i].eflags;
        }
        state->eflags = eflags;
    } else {
        // Simple copy when eflags match
        *dptr = *sptr;
    }

    // Handle different tuplestore states
    switch (state->status) {
        case TSS_INMEM:
        case TSS_WRITEFILE:
            // No additional work needed
            break;

        case TSS_READFILE:
            // Handle file-based storage with active pointer considerations
            if (destptr == state->activeptr) {
                // Setting the active pointer requires a file seek
                if (dptr->eof_reached) {
                    BufFileSeek(state->myfile, state->writepos_file,
                               state->writepos_offset, SEEK_SET);
                } else {
                    BufFileSeek(state->myfile, dptr->file, dptr->offset, SEEK_SET);
                }
            } else if (srcptr == state->activeptr) {
                // Copying from active pointer requires getting current file position
                if (!dptr->eof_reached) {
                    BufFileTell(state->myfile, &dptr->file, &dptr->offset);
                }
            }
            break;

        default:
            elog(ERROR, "invalid tuplestore state");
            break;
    }
}
```