# bbsink_begin_backup

## Location
[src/include/backup/basebackup_sink.h:175-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/basebackup_sink.h#L175-L190)

## Overview
Initializes a backup sink by setting up the state and buffer configuration, then calling the sink's begin_backup operation.

## Definition

```c
static inline void
bbsink_begin_backup(bbsink *sink, bbsink_state *state, int buffer_length)
```
## Detailed Description
This inline function serves as the initialization entry point for PostgreSQL's base backup sink mechanism. It performs essential setup operations for a backup sink, including state assignment, buffer length configuration, and invoking the sink-specific begin_backup operation. The function includes assertions to ensure proper initialization conditions and validates that the sink's buffer is properly allocated with block-aligned length after the operation completes.

## Parameters / Member Variables
- `*sink`: Pointer to the backup sink structure that will handle the backup operations
- `*state`: Pointer to the backup state structure containing backup session information
- `buffer_length`: Size of the buffer to allocate for backup data transfer (must be > 0)
## Dependencies
- Functions called/Symbols referenced:
  - [bbsink](bbsink.md) (structure type)
  - [bbsink_state](bbsink_state.md) (structure type)
  - Assert (assertion macro)
  - BLCKSZ (block size constant)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [bbsink_gzip_begin_backup](bbsink_gzip_begin_backup.md)
  - [bbsink_lz4_begin_backup](bbsink_lz4_begin_backup.md)
  - [bbsink_forward_begin_backup](bbsink_forward_begin_backup.md)
  - [bbsink_zstd_begin_backup](bbsink_zstd_begin_backup.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Includes pre and post-condition assertions for robust error checking
- Ensures buffer length is aligned to PostgreSQL's block size (BLCKSZ) after sink initialization
- The function delegates actual implementation to sink-specific begin_backup operation through function pointer
- Part of PostgreSQL's pluggable backup sink architecture allowing different compression and destination formats

## Simplified Source

```c
// Simplified version of bbsink_begin_backup
static inline void bbsink_begin_backup(bbsink *sink, bbsink_state *state, int buffer_length) {
    Assert(sink != NULL);
    Assert(buffer_length > 0);

    // Set up sink state and buffer configuration
    sink->bbs_state = state;
    sink->bbs_buffer_length = buffer_length;
    sink->bbs_ops->begin_backup(sink);

    // Validate post-conditions
    Assert(sink->bbs_buffer != NULL);
    Assert((sink->bbs_buffer_length % BLCKSZ) == 0);
}
```

Key simplifications made:
- Removed detailed comments while preserving essential setup logic
- Maintained pre and post-condition assertions for safety
- Preserved state and buffer configuration setup
- Kept delegation to sink-specific implementation
- Maintained block size alignment validation