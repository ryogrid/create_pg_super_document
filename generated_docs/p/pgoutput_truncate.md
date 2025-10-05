# pgoutput_truncate

## Location
[src/backend/replication/pgoutput/pgoutput.c:1598-1665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1598-L1665)

## Overview
Processes and sends TRUNCATE operations over the wire during logical replication, handling multiple relations in a single truncate command.

## Definition

```c
static void
pgoutput_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  int nrelations, Relation relations[], ReorderBufferChange *change)
```
## Detailed Description
The  function handles the replication of TRUNCATE operations in PostgreSQL's logical replication system. Unlike regular DML operations that affect individual rows, TRUNCATE operations can affect multiple tables simultaneously and remove all data from specified relations.

Key functionality includes:
1. **Multi-relation Processing**: Iterates through all relations specified in the TRUNCATE command
2. **Publication Filtering**: Checks each relation for publishability and truncate publication permissions
3. **Partition Handling**: Properly handles partitioned tables based on publication configuration
4. **Cascade and Sequence Options**: Preserves TRUNCATE-specific options like CASCADE and RESTART IDENTITY
5. **Batch Processing**: Collects all valid relations and sends them in a single logical replication message
6. **Transaction Management**: Ensures proper transaction boundaries and schema synchronization

The function only sends the TRUNCATE message if at least one relation qualifies for replication.

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext containing output plugin state and configuration
- `*txn`: ReorderBufferTXN representing the current transaction being processed
- `nrelations`: Number of relations involved in the TRUNCATE operation
- `relations[]`: Array of Relation objects representing the tables being truncated
- `*change`: ReorderBufferChange containing TRUNCATE-specific options (cascade, restart_seqs)
## Dependencies
- Functions called/Symbols referenced:
  - [is_publishable_relation](../i/is_publishable_relation.md)
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)
  - [pgoutput_send_begin](pgoutput_send_begin.md)
  - [maybe_send_schema](../m/maybe_send_schema.md)
  - [logicalrep_write_truncate](../l/logicalrep_write_truncate.md)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (as callback registration)

## Notes and Other Information
- Supports both CASCADE and RESTART IDENTITY options from the original TRUNCATE command
- Properly handles partitioned tables by respecting the publish_as_relid configuration
- Only sends schema information for relations that will actually be included in the TRUNCATE message
- Uses a separate memory context that is reset after processing to prevent memory leaks
- Can handle empty TRUNCATE operations (where no relations qualify for publication) without sending unnecessary messages
- Maintains transaction consistency by ensuring BEGIN messages are sent before the TRUNCATE operation

## Simplified Source

```c
static void
pgoutput_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                  int nrelations, Relation relations[], ReorderBufferChange *change)
{
    PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;
    PGOutputTxnData *txndata = (PGOutputTxnData *) txn->output_plugin_private;
    RelationSyncEntry *relentry;
    Oid *relids;
    int nrelids = 0;
    TransactionId xid = InvalidTransactionId;

    // Get transaction ID for streaming mode
    if (data->in_streaming)
        xid = change->txn->xid;

    MemoryContext old = MemoryContextSwitchTo(data->context);
    relids = palloc0(nrelations * sizeof(Oid));

    // Process each relation in the TRUNCATE command
    for (int i = 0; i < nrelations; i++)
    {
        Relation relation = relations[i];
        Oid relid = RelationGetRelid(relation);

        // Check if relation is publishable
        if (!is_publishable_relation(relation))
            continue;

        relentry = get_rel_sync_entry(data, relation);

        // Check if TRUNCATE is allowed for this publication
        if (!relentry->pubactions.pubtruncate)
            continue;

        // Skip partitions if publishing via root table
        if (relation->rd_rel->relispartition &&
            relentry->publish_as_relid != relid)
            continue;

        // Add relation to list of relations to truncate
        relids[nrelids++] = relid;

        // Send BEGIN if haven't sent it yet
        if (txndata && !txndata->sent_begin_txn)
            pgoutput_send_begin(ctx, txn);

        // Send schema information if needed
        maybe_send_schema(ctx, change, relation, relentry);
    }

    // Send TRUNCATE message if we have relations to truncate
    if (nrelids > 0)
    {
        OutputPluginPrepareWrite(ctx, true);
        logicalrep_write_truncate(ctx->out,
                                 xid,
                                 nrelids,
                                 relids,
                                 change->data.truncate.cascade,
                                 change->data.truncate.restart_seqs);
        OutputPluginWrite(ctx, true);
    }

    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}
```