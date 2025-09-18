# bbsink_lz4_begin_backup

## Location
src/backend/backup/basebackup_lz4.c: 93 - 131

## Overview
Initializes the LZ4 compression sink for beginning a base backup operation, setting up compression preferences and buffer allocations.

## Definition
```c
static void bbsink_lz4_begin_backup(bbsink *sink)
```

## Detailed Description
This function performs the initialization tasks required when starting a base backup with LZ4 compression. It sets up the LZ4 compression preferences including block size (256KB) and compression level, allocates the necessary input buffer for the sink, calculates the required output buffer size based on LZ4's compression bounds, and initializes the next sink in the chain with the appropriate buffer size.

The function ensures that the output buffer size accommodates the worst-case compression scenario by using `LZ4F_compressBound()` and rounds up to the next BLCKSZ multiple for alignment requirements. This careful buffer management is crucial for the streaming compression process.

## Parameters / Member Variables
- `sink`: Pointer to the base bbsink structure (cast to bbsink_lz4 internally)

## Dependencies
- Functions called/Symbols referenced:
  - memset (memory initialization)
  - [palloc](../p/palloc.md) (memory allocation)  
  - LZ4F_compressBound (LZ4 library function)
  - bbsink_begin_backup (calls next sink in chain)
- Called from (representative examples):
  - Referenced through bbsink_lz4_ops function pointer table

## Notes and Other Information
- Static function, only accessible within the basebackup_lz4.c module
- Sets LZ4 block size to maximum (256KB) for better compression ratios
- Carefully manages buffer sizes to meet LZ4 compression requirements
- Rounds output buffer to BLCKSZ boundaries for PostgreSQL block alignment
- Part of the sink operation callbacks, called through function pointer indirection