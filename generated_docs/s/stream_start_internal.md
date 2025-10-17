# stream_start_internal

## Location
[src/backend/replication/logical/worker.c:1431-1468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1431-L1468)

## Overview
Initializes the file-based streaming infrastructure for large transactions in PostgreSQL logical replication, setting up persistent storage for transaction data that exceeds memory limits.

## Definition
void stream_start_internal(TransactionId xid, bool first_segment)

## Detailed Description
stream_start_internal is a core function that manages the initialization of file-based streaming infrastructure for large transactions in PostgreSQL logical replication. When transactions are too large to fit in memory, they must be spooled to disk using a fileset mechanism. This function handles both the one-time initialization of the worker's FileSet and the per-transaction setup of spool files.

The function performs lazy initialization of the worker's stream_fileset - it only creates the FileSet when the first streaming message arrives, ensuring resources are not allocated unnecessarily. Once created, the FileSet persists for the entire duration of the worker and is used for all streaming transactions.

For each transaction, the function opens an appropriate spool file and, if this is not the first segment of a streaming transaction, it also reads existing subtransaction information to maintain proper transaction state across segments.

## Parameters / Member Variables
- : TransactionId of the streaming transaction that needs file-based storage
- : Boolean indicating whether this is the first segment of the streaming transaction

## Dependencies
- Functions called/Symbols referenced:
  - [begin_replication_step](../b/begin_replication_step.md)
  - [FileSetInit](../F/FileSetInit.md)
  - [stream_open_file](stream_open_file.md)
  - [subxact_info_read](subxact_info_read.md)
  - [end_replication_step](../e/end_replication_step.md)
- Called from:
  - [pa_switch_to_partial_serialize](../p/pa_switch_to_partial_serialize.md)
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md)
  - [stream_open_and_write_change](stream_open_and_write_change.md)

## Notes and Other Information
- Implements lazy initialization pattern for FileSet to avoid unnecessary resource allocation
- Uses ApplyContext for persistent memory allocation since FileSet must survive for worker lifetime
- The FileSet is shared across all streaming transactions handled by the worker
- Handles both initial segment creation and continuation of existing streaming transactions
- Subtransaction information is preserved across segments for proper transaction state management
- Part of PostgreSQL's memory management strategy for large logical replication transactions
- The replication step lifecycle is managed to ensure proper transaction boundaries
- Critical for handling transactions that exceed the logical_decoding_work_mem limit

## Simplified Source

```c
void stream_start_internal(TransactionId xid, bool first_segment)
{
    begin_replication_step();

    // Initialize worker's fileset on first use (lazy initialization)
    if (!MyLogicalRepWorker->stream_fileset) {
        // Switch to permanent context for worker lifetime allocation
        MemoryContext oldctx = MemoryContextSwitchTo(ApplyContext);

        MyLogicalRepWorker->stream_fileset = palloc(sizeof(FileSet));
        FileSetInit(MyLogicalRepWorker->stream_fileset);

        MemoryContextSwitchTo(oldctx);
    }

    // Open spool file for this transaction
    stream_open_file(MyLogicalRepWorker->subid, xid, first_segment);

    // For continuation segments, read existing subtransaction info
    if (!first_segment)
        subxact_info_read(MyLogicalRepWorker->subid, xid);

    end_replication_step();
}
```