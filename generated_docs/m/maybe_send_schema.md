# maybe_send_schema

## Location
src/backend/replication/pgoutput/pgoutput.c: 705 - 775

## Overview
Conditionally sends the schema of a relation and its ancestor (if any) to the logical replication stream, ensuring schema information is transmitted before data changes.

## Definition


## Detailed Description
This function determines whether to send schema information for a relation in the logical replication stream. It handles both streaming and non-streaming transactions, tracking which schemas have already been sent to avoid redundant transmissions. The function has special handling for relations that are published using an ancestor's schema (inheritance scenarios), sending both the ancestor's and the relation's schema information when needed. For streaming transactions, schema tracking is maintained per transaction to handle the complex ordering requirements of streamed vs non-streamed transactions.

## Parameters / Member Variables
- : LogicalDecodingContext pointer containing the replication context and output stream
- : ReorderBufferChange pointer representing the current change being processed
- : Relation representing the table whose schema might need to be sent
- : RelationSyncEntry pointer containing synchronization state and metadata for the relation

## Dependencies
- Functions called/Symbols referenced:
  - rbtxn_is_subtxn
  - rbtxn_get_toptxn
  - get_schema_sent_in_streamed_txn
  - RelationIdGetRelation
  - send_relation_and_attrs
  - RelationClose
  - set_schema_sent_in_streamed_txn
  - PGOutputData (type)
  - LogicalDecodingContext (type)
  - ReorderBufferChange (type)
  - RelationSyncEntry (type)
- Called from (representative examples):
  - pgoutput_change
  - pgoutput_truncate

## Notes and Other Information
This function includes an optimization comment noting that schema sending in streaming transactions could potentially be improved by checking the 'relentry->schema_sent' flag, but this needs careful analysis for mixed streaming/non-streaming transaction scenarios. The function handles inheritance hierarchies by sending ancestor schemas first when a relation is published using an ancestor's schema definition. Schema tracking is maintained separately for streaming and non-streaming contexts due to different transaction visibility and ordering requirements.