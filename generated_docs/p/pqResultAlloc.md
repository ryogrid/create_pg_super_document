# pqResultAlloc

## Location
src/interfaces/libpq/fe-exec.c: 563 - 662

## Overview
pqResultAlloc is an internal function that allocates subsidiary storage for a PGresult object with sophisticated memory management including alignment control and block allocation strategies.

## Definition
```c
void *pqResultAlloc(PGresult *res, size_t nBytes, bool isBinary)
```

## Detailed Description
pqResultAlloc is the core memory allocation function for PGresult objects. It implements a block-based memory management system that can handle both binary and text data with appropriate alignment. The function uses a current block strategy for efficient small allocations, creates separate large blocks for oversized requests, and automatically manages block chaining. It supports both aligned binary data allocation and unaligned text allocation to optimize memory usage.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure where memory should be allocated
- `nBytes`: Size in bytes of the memory block to allocate  
- `isBinary`: Boolean flag indicating whether binary alignment is required

## Dependencies
- Functions called/Symbols referenced:
  - PGresult_data
  - PGRESULT_ALIGN_BOUNDARY
  - PGRESULT_SEP_ALLOC_THRESHOLD
  - PGRESULT_BLOCK_OVERHEAD
  - PGRESULT_DATA_BLOCKSIZE
  - malloc
- Called from (representative examples):
  - PQresultAlloc
  - PQsetvalue
  - pqResultStrdup
  - pqInternalNotice
  - pqSaveMessageField
  - pqRowProcessor
  - getRowDescriptions
  - getParamDescriptions
  - getCopyStart

## Notes and Other Information
- Implements block-based memory management with linked list of data blocks
- Large allocations (>= PGRESULT_SEP_ALLOC_THRESHOLD) get separate blocks to avoid fragmentation
- Binary data gets proper alignment, text data can be allocated on any byte boundary
- Tracks total memory usage in res->memorySize for monitoring purposes
- Returns res->null_field for zero-byte allocations
- Located at src/interfaces/libpq/fe-exec.c:563-662