# SlruCtlData

## Location
src/include/access/slru.h: 127 - 164

## Overview
SlruCtlData is a process-local control structure that provides configuration and access to PostgreSQL's SLRU (Simple LRU) shared memory structures, containing both shared memory pointers and local configuration parameters.

## Definition


## Detailed Description
SlruCtlData serves as the primary control structure for individual SLRU instances in PostgreSQL. Unlike SlruSharedData which resides in shared memory, SlruCtlData is a process-local structure that contains configuration parameters and a pointer to the corresponding shared memory region. Each SLRU subsystem (CLOG, commit timestamps, multixact, subtransactions, etc.) maintains its own SlruCtlData instance.

The structure defines key operational parameters including the banking configuration, file naming conventions, synchronization handlers, and page ordering logic. It acts as the interface between the generic SLRU implementation and specific subsystem requirements, allowing different SLRU instances to customize behavior while sharing the common buffer management infrastructure.

## Parameters / Member Variables
- : SlruShared - Pointer to the corresponding SlruSharedData structure in shared memory
- : uint16 - Number of banks used for this SLRU instance, affecting lock granularity and concurrency
- : bool - Controls file naming format; true for long segment names, false for short names (see SlruFileName)
- : SyncRequestHandler - Specifies which sync handler to use for fsync requests to the checkpointer (SYNC_HANDLER_NONE to disable)
- : Function pointer - Callback function determining page ordering for truncation and LRU eviction; must use modular arithmetic for SLRUs using SimpleLruTruncate
- : char[64] - Directory path for SLRU files, set during initialization and never changed thereafter

## Dependencies
- Functions called/Symbols referenced:
  - SlruShared (pointer to shared data type)
  - SyncRequestHandler (enumeration for sync handling)
  - [SlruSharedData](SlruSharedData.md) (referenced through shared pointer)

- Called from (representative examples):
  - CLOG operations (clog.c:108)
  - Commit timestamp operations (commit_ts.c:83)
  - MultiXact operations (multixact.c:228-229)
  - Subtransaction operations (subtrans.c:72)
  - Async notification queue (async.c:308)
  - Predicate locking (predicate.c:324)
  - Test modules (test_slru.c:43)

## Notes and Other Information
- This is a process-local structure, not shared between processes
- Each SLRU subsystem (CLOG, multixact, subtrans, etc.) has its own SlruCtlData instance
- The Dir field is immutable after initialization, eliminating the need for it to be in shared memory
- The PagePrecedes callback enables different ordering semantics for different SLRU types
- Banking configuration (nbanks) directly affects concurrency characteristics
- File naming strategy affects disk storage organization and backward compatibility
- Used as SlruCtl typedef (pointer to SlruCtlData) throughout the codebase
- The structure bridges generic SLRU infrastructure with subsystem-specific requirements
- Sync handler selection allows fine-grained control over fsync behavior per SLRU instance