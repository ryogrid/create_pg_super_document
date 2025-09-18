# send_relation_and_attrs

## Location
src/backend/replication/pgoutput/pgoutput.c: 776 - 819

## Overview
Sends relation schema information and associated data type definitions to the logical replication stream, including only user-created types and specified columns.

## Definition


## Detailed Description
This function transmits schema information for a relation to logical replication subscribers. It first iterates through the relation's attributes and sends type information for user-created data types (those with OIDs >= FirstGenbkiObjectId), excluding built-in PostgreSQL types that are expected to be stable across versions. The function skips dropped and generated columns, and respects column filtering when a specific column set is provided. After sending necessary type definitions, it sends the complete relation schema using the logical replication protocol.

## Parameters / Member Variables
- : Relation representing the table whose schema is being sent
- : TransactionId of the current transaction (may be InvalidTransactionId for non-transactional contexts)
- : LogicalDecodingContext pointer containing the replication context and output stream
- : Bitmapset pointer specifying which columns to include (NULL means all columns)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr (via TupleDescAttr macro)
  - TupleDescAttr
  - bms_is_member
  - OutputPluginPrepareWrite
  - logicalrep_write_typ
  - OutputPluginWrite
  - logicalrep_write_rel
  - FirstGenbkiObjectId (constant)
  - LogicalDecodingContext (type)
- Called from (representative examples):
  - maybe_send_schema (twice - for ancestor and relation)

## Notes and Other Information
The function uses FirstGenbkiObjectId as a cutoff to distinguish between built-in types (with hand-assigned OIDs that remain stable across PostgreSQL versions) and user-created types that need explicit transmission. This is crucial for cross-version replication compatibility. The function handles column filtering through the Bitmapset parameter, allowing for selective replication of specific columns. Type information is sent before relation schema to ensure subscribers have all necessary type definitions before processing the relation structure.