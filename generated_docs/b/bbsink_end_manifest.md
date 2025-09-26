# bbsink_end_manifest

## Location
[src/include/backup/basebackup_sink.h:246-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/basebackup_sink.h#L246-L254)

## Overview
Signals the completion of backup manifest transmission, allowing sink implementations to finalize manifest-specific operations and cleanup.

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
This inline function serves as the interface for concluding the backup manifest transmission phase. It delegates to sink-specific implementations to perform any necessary finalization operations for the backup manifest. This may include flushing buffers, finalizing compression, closing files, or performing other cleanup tasks specific to the sink type.

The function is called after all manifest content has been processed through bbsink_manifest_contents calls, marking the end of the manifest transmission phase in the base backup process. This allows different sink implementations to handle manifest finalization according to their specific requirements while maintaining a consistent interface.

## Parameters / Member Variables
- : Pointer to the bbsink object that should finalize manifest processing. Must not be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (struct type)
  - Assert (assertion macro)
  - sink->bbs_ops->end_manifest (callback function)

- Called from (representative examples):
  - SendBackupManifest
  - bbsink_forward_end_manifest

## Notes and Other Information  
- This is an inline function defined in the header file for performance
- Called as the final step in the manifest transmission sequence: bbsink_begin_manifest → bbsink_manifest_contents (multiple calls) → bbsink_end_manifest
- Part of the base backup infrastructure ensuring proper cleanup and finalization of manifest operations
- The actual finalization behavior depends on the specific sink implementation (compression, networking, file I/O, etc.)
- Critical for maintaining data integrity and proper resource management during backup manifest transmission