# bbsink_begin_manifest

## Location
[src/include/backup/basebackup_sink.h:225-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/basebackup_sink.h#L225-L233)

## Overview
Initiates the creation of a backup manifest within a backup sink by calling the sink-specific manifest initialization operation.

## Definition

```c
static inline void
bbsink_begin_manifest(bbsink *sink)
```
## Detailed Description
This inline function serves as a wrapper to begin the backup manifest generation within the PostgreSQL base backup system. The backup manifest is a critical component that contains metadata about all files included in the backup, their checksums, and other integrity information. The function delegates to the sink-specific begin_manifest operation, which handles the actual initialization of manifest creation according to the sink's output format and destination requirements.

## Parameters / Member Variables
- `*sink`: Pointer to the backup sink structure that will handle the manifest operations
## Dependencies
- Functions called/Symbols referenced:
  - [bbsink](bbsink.md) (structure type)
  - Assert (assertion macro)
- Called from (representative examples):
  - [SendBackupManifest](../S/SendBackupManifest.md)
  - [bbsink_forward_begin_manifest](bbsink_forward_begin_manifest.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Includes assertion to ensure sink is not NULL for defensive programming
- The function delegates actual manifest initialization to sink-specific implementation through function pointer
- Part of PostgreSQL's pluggable backup sink architecture allowing different output formats
- The backup manifest provides crucial integrity verification capabilities for backup validation
- Called as part of the backup completion process to generate manifest metadata
- Works in conjunction with other manifest-related functions to create comprehensive backup metadata
- Essential for backup verification and integrity checking in PostgreSQL's backup system

## Simplified Source

```c
static inline void bbsink_begin_manifest(bbsink *sink)
{
    Assert(sink != NULL);

    // Delegate to the sink-specific manifest initialization
    sink->bbs_ops->begin_manifest(sink);
}
```