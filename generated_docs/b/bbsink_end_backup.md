# bbsink_end_backup

## Location
[src/include/backup/basebackup_sink.h:255-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/basebackup_sink.h#L255-L264)

## Overview
Signals the completion of an entire base backup operation, allowing sink implementations to perform final cleanup and record the backup's end point in the WAL stream.

## Definition

```c
struction. */
static inline void
bbsink_cleanup(bbsink *sink)
{
	Assert(sink != NULL);

	sink->bbs_ops->cleanup(sink);
}

/* Forwarding callbacks. Use these to pass operations through to next sink. */
extern void bbsink_forward_begin_backup(bbsink *sink);
```
## Detailed Description
This inline function marks the successful completion of a base backup operation. It is called after all tablespace archives and the backup manifest have been transmitted. The function delegates to sink-specific implementations to perform final operations such as recording the backup end position, finalizing any remaining data structures, and conducting final validation.

The function includes an assertion to verify that all tablespaces have been processed correctly (tablespace_num equals the total number of tablespaces), ensuring the backup process has completed all required components before finalizing.

## Parameters / Member Variables
- : Pointer to the bbsink object that should finalize the backup operation. Must not be NULL.
- : The WAL position where the backup ended, used for recovery and consistency verification.
- : The timeline ID at the backup end point, ensuring proper timeline tracking for recovery.

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (struct type)
  - XLogRecPtr (WAL position type)
  - TimeLineID (timeline identifier type)
  - Assert (assertion macro)
  - list_length (list utility function)
  - sink->bbs_ops->end_backup (callback function)

- Called from (representative examples):
  - perform_base_backup
  - bbsink_forward_end_backup

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Called as the final step in the complete backup sequence after all archives and manifest have been processed
- The endptr and endtli parameters are critical for backup consistency and recovery procedures
- Includes validation that ensures all tablespaces were properly processed during the backup
- Part of the base backup infrastructure ensuring proper finalization and consistency recording
- The actual finalization behavior depends on the specific sink implementation but typically includes recording backup metadata and cleanup operations