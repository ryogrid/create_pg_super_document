# sts_end_write

## Location
src/backend/utils/sort/sharedtuplestore.c: 213 - 233

## Overview
Finalizes the writing phase for a participant by flushing any remaining data and cleaning up write-related resources.

## Definition


## Detailed Description
The  function must be called by all participants that have written data to the SharedTuplestore before any participant begins reading. It performs cleanup of the write state by flushing any remaining data in the write chunk buffer to the backing file, closing the write file handle, freeing the write chunk buffer memory, and marking the participant as no longer writing.

This function ensures that all written data is properly persisted and that the participant transitions cleanly from write mode, allowing the reading phase to begin safely.

## Parameters / Member Variables
- : SharedTuplestoreAccessor for the participant ending its write operations

## Dependencies
- Functions called/Symbols referenced:
  - SharedTuplestoreAccessor (struct type)
  - sts_flush_chunk
  - BufFileClose
  - write_chunk (accessor field)
- Called from (representative examples):
  - MultiExecParallelHash
  - ExecParallelHashCloseBatchAccessors
  - ExecHashTableDetach
  - ExecParallelHashJoinPartitionOuter
  - SHARED_TUPLESTORE_SINGLE_PASS

## Notes and Other Information
- This function is safe to call multiple times or when no writing has occurred (it checks for NULL write_file)
- All participants that write data must call this before any participant begins reading
- The function sets the participant's writing flag to false, which may be used for synchronization
- After calling this function, the accessor can no longer be used for writing operations
- The write chunk buffer memory is freed, so attempts to write after this call would be invalid
- This is part of the write-then-read pattern used in parallel execution where all workers write their data first, then collectively read and process it