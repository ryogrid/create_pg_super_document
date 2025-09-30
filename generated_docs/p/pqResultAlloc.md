# pqResultAlloc

## Location
[src/interfaces/libpq/fe-exec.c:563-662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L563-L662)

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
  - [PQresultAlloc](../P/PQresultAlloc.md)
  - [PQsetvalue](../P/PQsetvalue.md)
  - [pqResultStrdup](pqResultStrdup.md)
  - [pqInternalNotice](pqInternalNotice.md)
  - [pqSaveMessageField](pqSaveMessageField.md)
  - [pqRowProcessor](pqRowProcessor.md)
  - [getRowDescriptions](../g/getRowDescriptions.md)
  - [getParamDescriptions](../g/getParamDescriptions.md)
  - [getCopyStart](../g/getCopyStart.md)

## Notes and Other Information
- Implements block-based memory management with linked list of data blocks
- Large allocations (>= PGRESULT_SEP_ALLOC_THRESHOLD) get separate blocks to avoid fragmentation
- Binary data gets proper alignment, text data can be allocated on any byte boundary
- Tracks total memory usage in res->memorySize for monitoring purposes
- Returns res->null_field for zero-byte allocations
- Located at src/interfaces/libpq/fe-exec.c:563-662

## Simplified Source

```c
void *pqResultAlloc(PGresult *res, size_t nBytes, bool isBinary) {
    char *space;
    PGresult_data *block;

    // Basic validation
    if (!res || nBytes <= 0) {
        return res ? res->null_field : NULL;
    }

    // Handle binary alignment if needed
    if (isBinary) {
        int offset = res->curOffset % PGRESULT_ALIGN_BOUNDARY;
        if (offset) {
            res->curOffset += PGRESULT_ALIGN_BOUNDARY - offset;
            res->spaceLeft -= PGRESULT_ALIGN_BOUNDARY - offset;
        }
    }

    // Use current block if enough space available
    if (nBytes <= (size_t) res->spaceLeft) {
        space = res->curBlock->space + res->curOffset;
        res->curOffset += nBytes;
        res->spaceLeft -= nBytes;
        return space;
    }

    // Large allocations get their own block
    if (nBytes >= PGRESULT_SEP_ALLOC_THRESHOLD) {
        size_t alloc_size = nBytes + PGRESULT_BLOCK_OVERHEAD;
        block = (PGresult_data *) malloc(alloc_size);
        if (!block) return NULL;

        res->memorySize += alloc_size;
        space = block->space + PGRESULT_BLOCK_OVERHEAD;

        // Insert block into chain
        if (res->curBlock) {
            block->next = res->curBlock->next;
            res->curBlock->next = block;
        } else {
            block->next = NULL;
            res->curBlock = block;
            res->spaceLeft = 0;
        }
        return space;
    }

    // Allocate new standard block
    block = (PGresult_data *) malloc(PGRESULT_DATA_BLOCKSIZE);
    if (!block) return NULL;

    res->memorySize += PGRESULT_DATA_BLOCKSIZE;
    block->next = res->curBlock;
    res->curBlock = block;

    // Set up block offsets based on alignment needs
    if (isBinary) {
        res->curOffset = PGRESULT_BLOCK_OVERHEAD;
        res->spaceLeft = PGRESULT_DATA_BLOCKSIZE - PGRESULT_BLOCK_OVERHEAD;
    } else {
        res->curOffset = sizeof(PGresult_data);
        res->spaceLeft = PGRESULT_DATA_BLOCKSIZE - sizeof(PGresult_data);
    }

    space = block->space + res->curOffset;
    res->curOffset += nBytes;
    res->spaceLeft -= nBytes;
    return space;
}
```