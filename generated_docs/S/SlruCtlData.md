# SlruCtlData

## Location
[src/include/access/slru.h:127-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/slru.h#L127-L164)

## Overview
SlruCtlData is a process-local control structure that provides configuration and access to PostgreSQL's SLRU (Simple LRU) shared memory structures, containing both shared memory pointers and local configuration parameters.

## Definition

```c
typedef struct SlruCtlData
{
	SlruShared	shared;

	/* Number of banks in this SLRU. */
	uint16		nbanks;

	/*
	 * If true, use long segment file names.  Otherwise, use short file names.
	 *
	 * For details about the file name format, see SlruFileName().
	 */
	bool		long_segment_names;

	/*
	 * Which sync handler function to use when handing sync requests over to
	 * the checkpointer.  SYNC_HANDLER_NONE to disable fsync (eg pg_notify).
	 */
	SyncRequestHandler sync_handler;

	/*
	 * Decide whether a page is "older" for truncation and as a hint for
	 * evicting pages in LRU order.  Return true if every entry of the first
	 * argument is older than every entry of the second argument.  Note that
	 * !PagePrecedes(a,b) && !PagePrecedes(b,a) need not imply a==b; it also
	 * arises when some entries are older and some are not.  For SLRUs using
	 * SimpleLruTruncate(), this must use modular arithmetic.  (For others,
	 * the behavior of this callback has no functional implications.)  Use
	 * SlruPagePrecedesUnitTests() in SLRUs meeting its criteria.
	 */
	bool		(*PagePrecedes) (int64, int64);

	/*
	 * Dir is set during SimpleLruInit and does not change thereafter. Since
	 * it's always the same, it doesn't need to be in shared memory.
	 */
	char		Dir[64];
} SlruCtlData;
```
## Detailed Description
SlruCtlData serves as the primary control structure for individual SLRU instances in PostgreSQL. Unlike SlruSharedData which resides in shared memory, SlruCtlData is a process-local structure that contains configuration parameters and a pointer to the corresponding shared memory region. Each SLRU subsystem (CLOG, commit timestamps, multixact, subtransactions, etc.) maintains its own SlruCtlData instance.

The structure defines key operational parameters including the banking configuration, file naming conventions, synchronization handlers, and page ordering logic. It acts as the interface between the generic SLRU implementation and specific subsystem requirements, allowing different SLRU instances to customize behavior while sharing the common buffer management infrastructure.

## Parameters / Member Variables
- `shared`: SlruShared - Pointer to the corresponding SlruSharedData structure in shared memory
- `nbanks`: uint16 - Number of banks used for this SLRU instance, affecting lock granularity and concurrency
- `long_segment_names`: bool - Controls file naming format; true for long segment names, false for short names (see SlruFileName)
- `sync_handler`: SyncRequestHandler - Specifies which sync handler to use for fsync requests to the checkpointer (SYNC_HANDLER_NONE to disable)
- `int64)`: Function pointer - Callback function determining page ordering for truncation and LRU eviction; must use modular arithmetic for SLRUs using SimpleLruTruncate
- `Dir[64]`: char[64] - Directory path for SLRU files, set during initialization and never changed thereafter
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