# shm_toc_lookup

## Location
src/backend/storage/ipc/shm_toc.c: 232 - 262

## Overview
Searches for and retrieves a data chunk from a shared memory table of contents (TOC) using a specified key, with lock-free operation using memory barriers for thread safety.

## Definition


## Detailed Description
This function performs a lookup operation in a shared memory table of contents structure without acquiring locks, using memory barriers for synchronization. It iterates through the TOC entries to find a matching key and returns a pointer to the corresponding data chunk in shared memory. The function is designed for high-concurrency scenarios where multiple worker processes might need to read TOC entries simultaneously. If the key is not found, the function either returns NULL (when noError is true) or throws an ERROR-level log message (when noError is false).

## Parameters / Member Variables
- `toc`: Pointer to the shared memory table of contents structure to search in
- `key`: 64-bit key value to search for in the TOC entries
- `noError`: Boolean flag controlling error handling behavior - if true, returns NULL when key not found; if false, throws elog(ERROR)

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc (structure type)
  - pg_read_barrier (memory barrier function)
  - UINT64_FORMAT (macro for formatting 64-bit integers)
  - elog (error logging function)
- Called from (representative examples):
  - _brin_parallel_build_main
  - _bt_parallel_build_main
  - ParallelWorkerMain
  - parallel_vacuum_main
  - ExecParallelSetupTupleQueues
  - ParallelQueryMain
  - ParallelApplyWorkerMain

## Notes and Other Information
- This function is designed to be lock-free for performance reasons, using only memory barriers for synchronization
- The function assumes that reading a uint32 (toc->toc_nentry) is atomic
- Uses pg_read_barrier() to ensure proper ordering of memory reads
- Widely used throughout PostgreSQL's parallel execution infrastructure
- Returns a pointer calculated as an offset from the TOC base address
- The lock-free design is specifically optimized for scenarios where multiple worker processes read from the same TOC concurrently