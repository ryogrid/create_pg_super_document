# bbsink

## Location
[src/include/backup/basebackup_sink.h:36-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/basebackup_sink.h#L36-L36)

## Overview
A typedef for the base backup sink structure that provides an object-oriented framework for processing and forwarding PostgreSQL base backup archives and manifests through a chain of filtering and destination components.

## Definition

```c
typedef struct bbsink bbsink;
```
## Detailed Description
The  type is a forward declaration typedef for the  structure defined in the same header file. It represents a base backup sink object used in PostgreSQL's base backup process. The bbsink implements a chain-of-responsibility design pattern where backup data (archives and manifests) flows through a chain of bbsink objects, each responsible for a specific task such as compression, throttling, progress reporting, or final destination handling.

The bbsink structure contains callback operations (), a data buffer for processing backup content, a pointer to the next sink in the chain, and shared state information. This design allows for modular composition of backup processing stages while maintaining a consistent interface.

## Parameters / Member Variables
As a typedef, bbsink itself has no direct parameters, but the underlying  contains:
- : Pointer to callback table for sink operations
- : Buffer for storing backup data during processing  
- : Allocated length of the buffer
- : Pointer to next bbsink in the processing chain
- : Pointer to shared backup state object

## Dependencies
- Functions called/Symbols referenced:
  -  (the actual structure definition)
  -  (callback operations structure)
  -  (shared backup state structure)
- Called from (representative examples):
  -  in src/backend/backup/basebackup.c:234
  -  in src/backend/backup/basebackup.c:991
  - Various bbsink constructor functions (bbsink_gzip_new, bbsink_lz4_new, etc.)
  - All bbsink inline wrapper functions (bbsink_begin_backup, bbsink_archive_contents, etc.)

## Notes and Other Information
- This typedef enables forward declaration of the bbsink structure, allowing header files to reference bbsink pointers without requiring the full structure definition
- Part of PostgreSQL's base backup infrastructure introduced to provide a flexible, extensible framework for backup data processing
- The actual structure definition follows at lines 99-106 in the same header file
- Used extensively throughout the base backup subsystem to implement various data processing pipelines including compression, network transmission, and file I/O