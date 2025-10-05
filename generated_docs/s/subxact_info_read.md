# subxact_info_read

## Location
[src/backend/replication/logical/worker.c:4068-4118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4068-L4118)

## Overview
Restores subtransaction information from a file into memory for a streamed logical replication transaction.

## Definition
```c
static void subxact_info_read(Oid subid, TransactionId xid)
```

## Detailed Description
This function reads previously stored subtransaction information from a file back into the global subxact_data structure. It handles the complete restoration process, including memory allocation in the appropriate context (LogicalStreamingContext), file validation, and proper sizing of internal data structures. The function is designed to work with files created by subxact_info_write and gracefully handles the case where no subtransaction file exists.

## Parameters / Member Variables
- `subid`: Object ID of the subscription
- `xid`: Transaction ID of the toplevel transaction

## Dependencies
- Functions called/Symbols referenced:
  - [subxact_filename](subxact_filename.md)
  - [BufFileOpenFileSet](../B/BufFileOpenFileSet.md)
  - [BufFileReadExact](../B/BufFileReadExact.md)
  - [BufFileClose](../B/BufFileClose.md)
  - [my_log2](../m/my_log2.md)
  - [palloc](../p/palloc.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [SubXactInfo](../S/SubXactInfo.md)
  - [BufFile](../B/BufFile.md)
  - LogicalStreamingContext
- Called from (representative examples):
  - [stream_start_internal](stream_start_internal.md)
  - [stream_abort_internal](stream_abort_internal.md)

## Notes and Other Information
- This is a static function with internal linkage within worker.c
- The function includes assertions to ensure the subxact_data structure is in a clean state before reading
- Memory allocation occurs in LogicalStreamingContext to persist throughout the streaming session
- The maximum number of subtransactions is kept as a power of 2 for efficient memory management
- If no subtransaction file exists, the function returns early without error
- The function uses BufFileReadExact for reliable file reading operations
- Memory allocated here is later freed by cleanup_subxact_info() when the stream completes
- The implementation handles the case where len > 0 before reading actual subtransaction data

## Simplified Source

```c
static void subxact_info_read(Oid subid, TransactionId xid) {
    char path[MAXPGPATH];
    Size len;
    BufFile *fd;
    MemoryContext oldctx;

    Assert(!subxact_data.subxacts);
    Assert(subxact_data.nsubxacts == 0);
    Assert(subxact_data.nsubxacts_max == 0);

    // Generate filename and try to open subxact info file
    subxact_filename(path, subid, xid);
    fd = BufFileOpenFileSet(MyLogicalRepWorker->stream_fileset, path, O_RDONLY, true);

    // If file doesn't exist, no subxact info available
    if (fd == NULL)
        return;

    // Read number of subtransactions
    BufFileReadExact(fd, &subxact_data.nsubxacts, sizeof(subxact_data.nsubxacts));

    len = sizeof(SubXactInfo) * subxact_data.nsubxacts;

    // Set maximum as power of 2 for efficient memory management
    subxact_data.nsubxacts_max = 1 << my_log2(subxact_data.nsubxacts);

    // Allocate memory in LogicalStreamingContext
    oldctx = MemoryContextSwitchTo(LogicalStreamingContext);
    subxact_data.subxacts = palloc(subxact_data.nsubxacts_max * sizeof(SubXactInfo));
    MemoryContextSwitchTo(oldctx);

    // Read actual subtransaction data if any exists
    if (len > 0)
        BufFileReadExact(fd, subxact_data.subxacts, len);

    BufFileClose(fd);
}
```