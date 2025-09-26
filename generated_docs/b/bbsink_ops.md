# bbsink_ops

## Location
src/include/backup/basebackup_sink.h: 37 - 65

## Overview
A typedef for the callback operations structure that defines the interface methods for base backup sink objects, enabling polymorphic behavior in the bbsink chain-of-responsibility pattern.

## Definition

```c
typedef struct bbsink_ops bbsink_ops;
```
## Detailed Description
The  type is a forward declaration typedef for the  structure that contains function pointers defining the operations interface for bbsink objects. This structure implements the strategy pattern, allowing different types of bbsink implementations to provide their own specific behavior for backup processing while maintaining a consistent interface.

The actual  (defined at lines 118-171) contains function pointers for all phases of backup processing: begin_backup, begin_archive, archive_contents, end_archive, begin_manifest, manifest_contents, end_manifest, end_backup, and cleanup. Each bbsink implementation must provide all these callbacks to handle the complete backup lifecycle.

## Parameters / Member Variables
As a typedef, bbsink_ops itself has no direct parameters, but the underlying  contains function pointers:
- : Callback invoked at the start of backup to initialize buffer
- : Callback invoked when starting to process a new archive
- : Callback invoked to process archive data chunks
- : Callback invoked when finishing an archive
- : Callback invoked when starting to process backup manifest
- : Callback invoked to process manifest data chunks
- : Callback invoked when finishing the manifest
- : Callback invoked at the end of the entire backup
- : Callback invoked to release resources (called on error or after end_backup)

## Dependencies
- Functions called/Symbols referenced:
  -  (the actual structure definition)
  -  (the sink structure that contains this ops pointer)
  -  and  (for end_backup callback parameters)
- Called from (representative examples):
  - All bbsink constructor functions (bbsink_copystream_new, bbsink_gzip_new, etc.)
  -  structure definition at src/include/backup/basebackup_sink.h:101
  - Various bbsink implementations to initialize their operation tables

## Notes and Other Information
- This typedef enables forward declaration of the bbsink_ops structure, supporting the separation of interface definition from implementation
- All callbacks in the structure are required - no optional callbacks are allowed
- Forwarding callbacks (bbsink_forward_*) are provided for implementations that simply need to pass operations to the next sink in the chain
- The design supports composition where one sink can wrap another, allowing for layered functionality like compression + progress reporting + network transmission
- Each bbsink implementation typically defines a static const bbsink_ops structure with its specific callback implementations