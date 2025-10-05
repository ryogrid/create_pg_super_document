# maybe_send_schema

## Location
[src/backend/replication/pgoutput/pgoutput.c:705-775](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L705-L775)

## Overview
Conditionally sends the schema of a relation and its ancestor (if any) to the logical replication stream, ensuring schema information is transmitted before data changes.

## Definition

```c
static void
maybe_send_schema(LogicalDecodingContext *ctx,
				  ReorderBufferChange *change,
				  Relation relation, RelationSyncEntry *relentry)
```
## Detailed Description
This function determines whether to send schema information for a relation in the logical replication stream. It handles both streaming and non-streaming transactions, tracking which schemas have already been sent to avoid redundant transmissions. The function has special handling for relations that are published using an ancestor's schema (inheritance scenarios), sending both the ancestor's and the relation's schema information when needed. For streaming transactions, schema tracking is maintained per transaction to handle the complex ordering requirements of streamed vs non-streamed transactions.

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext pointer containing the replication context and output stream
- `*change`: ReorderBufferChange pointer representing the current change being processed
- `relation`: Relation representing the table whose schema might need to be sent
- `*relentry`: RelationSyncEntry pointer containing synchronization state and metadata for the relation
## Dependencies
- Functions called/Symbols referenced:
  - rbtxn_is_subtxn
  - rbtxn_get_toptxn
  - [get_schema_sent_in_streamed_txn](../g/get_schema_sent_in_streamed_txn.md)
  - [RelationIdGetRelation](../R/RelationIdGetRelation.md)
  - [send_relation_and_attrs](../s/send_relation_and_attrs.md)
  - [RelationClose](../R/RelationClose.md)
  - [set_schema_sent_in_streamed_txn](../s/set_schema_sent_in_streamed_txn.md)
  - [PGOutputData](../P/PGOutputData.md) (type)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (type)
  - [ReorderBufferChange](../R/ReorderBufferChange.md) (type)
  - [RelationSyncEntry](../R/RelationSyncEntry.md) (type)
- Called from (representative examples):
  - [pgoutput_change](../p/pgoutput_change.md)
  - [pgoutput_truncate](../p/pgoutput_truncate.md)

## Notes and Other Information
This function includes an optimization comment noting that schema sending in streaming transactions could potentially be improved by checking the 'relentry->schema_sent' flag, but this needs careful analysis for mixed streaming/non-streaming transaction scenarios. The function handles inheritance hierarchies by sending ancestor schemas first when a relation is published using an ancestor's schema definition. Schema tracking is maintained separately for streaming and non-streaming contexts due to different transaction visibility and ordering requirements.

## Simplified Source

```c
static void
maybe_send_schema(LogicalDecodingContext *ctx,
                  ReorderBufferChange *change,
                  Relation relation, RelationSyncEntry *relentry)
{
    PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;
    bool schema_sent;
    TransactionId xid = InvalidTransactionId;
    TransactionId topxid = InvalidTransactionId;

    // Determine transaction IDs for streaming context
    if (data->in_streaming)
        xid = change->txn->xid;

    if (rbtxn_is_subtxn(change->txn))
        topxid = rbtxn_get_toptxn(change->txn)->xid;
    else
        topxid = xid;

    // Check if schema already sent (different tracking for streaming vs non-streaming)
    if (data->in_streaming)
        schema_sent = get_schema_sent_in_streamed_txn(relentry, topxid);
    else
        schema_sent = relentry->schema_sent;

    if (schema_sent)
        return;

    // Send ancestor schema first if relation uses ancestor's schema
    if (relentry->publish_as_relid != RelationGetRelid(relation)) {
        Relation ancestor = RelationIdGetRelation(relentry->publish_as_relid);
        send_relation_and_attrs(ancestor, xid, ctx, relentry->columns);
        RelationClose(ancestor);
    }

    // Send relation's own schema
    send_relation_and_attrs(relation, xid, ctx, relentry->columns);

    // Mark schema as sent
    if (data->in_streaming)
        set_schema_sent_in_streamed_txn(relentry, topxid);
    else
        relentry->schema_sent = true;
}
```