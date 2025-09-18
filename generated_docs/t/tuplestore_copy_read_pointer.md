# tuplestore_copy_read_pointer

## Location
src/backend/utils/sort/tuplestore.c: 1268 - 1359

## Overview
Copies the state of one read pointer to another read pointer within a tuplestore, handling different storage states and maintaining file position consistency.

## Definition


## Detailed Description
This function copies all the state information from a source read pointer to a destination read pointer within the same tuplestore. The operation handles different tuplestore states (in-memory, writing to file, reading from file) appropriately. When copying pointers in file-based mode, it must carefully manage file seek positions since the active read pointer's position corresponds to the actual file seek point rather than just the stored variables. The function also recomputes the overall eflags if the destination pointer's flags differ from the source.

## Parameters / Member Variables
- `state`: The tuplestore state containing the read pointers
- `srcptr`: Index of the source read pointer to copy from
- `destptr`: Index of the destination read pointer to copy to

## Dependencies
- Functions called/Symbols referenced:
  - Tuplestorestate
  - TSReadPointer
  - TSS_INMEM, TSS_WRITEFILE, TSS_READFILE (tuplestore status constants)
  - BufFileSeek
  - BufFileTell
- Called from (representative examples):
  - ExecMaterialMarkPos
  - ExecMaterialRestrPos

## Notes and Other Information
- The function performs validation to ensure both pointer indices are within valid range
- Self-assignment (srcptr == destptr) is treated as a no-op
- When eflags differ between pointers, the function recomputes the overall state eflags by OR-ing all read pointer eflags
- In TSS_READFILE state, special handling is required for the active read pointer since its logical position may differ from the file's actual seek position
- File operations can raise ERROR conditions if seek operations fail