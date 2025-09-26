# bbsink_end_archive

## Location
src/include/backup/basebackup_sink.h: 216 - 224

## Overview
Completes and finalizes an archive within a backup sink by calling the sink-specific archive completion operation.

## Definition

```c
static inline void
bbsink_end_archive(bbsink *sink)
```
## Detailed Description
This inline function serves as a wrapper to finalize and close a previously opened archive within the PostgreSQL base backup system. It delegates to the sink-specific end_archive operation, which handles the actual completion tasks such as flushing remaining data, writing archive footers, and performing cleanup operations. The function is a critical part of the backup sink lifecycle, ensuring proper archive closure and data integrity.

## Parameters / Member Variables
- : Pointer to the backup sink structure that will finalize the archive

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (structure type)
  - Assert (assertion macro)
- Called from (representative examples):
  - perform_base_backup
  - bbsink_forward_end_archive

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Includes assertion to ensure sink is not NULL for defensive programming
- The function delegates actual archive finalization to sink-specific implementation through function pointer
- Part of PostgreSQL's pluggable backup sink architecture allowing different compression and archiving formats
- Must be called after all archive contents have been processed to ensure proper archive closure
- Critical for maintaining data integrity and proper cleanup of archive resources
- Typically called in pairs with bbsink_begin_archive to bracket archive creation and completion