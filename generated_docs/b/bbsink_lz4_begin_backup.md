# bbsink_lz4_begin_backup

## Location
[src/backend/backup/basebackup_lz4.c:93-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_lz4.c#L93-L131)

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
  - [bbsink_begin_backup](bbsink_begin_backup.md) (calls next sink in chain)
- Called from (representative examples):
  - Referenced through bbsink_lz4_ops function pointer table

## Notes and Other Information
- Static function, only accessible within the basebackup_lz4.c module
- Sets LZ4 block size to maximum (256KB) for better compression ratios
- Carefully manages buffer sizes to meet LZ4 compression requirements
- Rounds output buffer to BLCKSZ boundaries for PostgreSQL block alignment
- Part of the sink operation callbacks, called through function pointer indirection

## Simplified Source

```c
static void
bbsink_lz4_begin_backup(bbsink *sink)
{
    bbsink_lz4 *mysink = (bbsink_lz4 *) sink;
    size_t output_buffer_bound;
    LZ4F_preferences_t *prefs = &mysink->prefs;

    // Initialize LZ4 compression preferences
    memset(prefs, 0, sizeof(LZ4F_preferences_t));
    prefs->frameInfo.blockSizeID = LZ4F_max256KB;  // Use 256KB blocks
    prefs->compressionLevel = mysink->compresslevel;

    // Allocate input buffer for this sink
    mysink->base.bbs_buffer = palloc(mysink->base.bbs_buffer_length);

    // Calculate required output buffer size for compression
    output_buffer_bound = LZ4F_compressBound(mysink->base.bbs_buffer_length,
                                           &mysink->prefs);

    // Round up to BLCKSZ boundary for PostgreSQL alignment
    output_buffer_bound = output_buffer_bound + BLCKSZ -
        (output_buffer_bound % BLCKSZ);

    // Initialize next sink in chain with calculated buffer size
    bbsink_begin_backup(sink->bbs_next, sink->bbs_state, output_buffer_bound);
}
```