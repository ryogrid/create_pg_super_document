# RelationSyncEntry

## Location
src/backend/replication/pgoutput/pgoutput.c: 132 - 185

## Overview
RelationSyncEntry is a struct used in PostgreSQL's logical replication pgoutput plugin to cache schema information and publication metadata for relations being replicated to subscribers.

## Definition


## Detailed Description
RelationSyncEntry serves as a cache entry in the pgoutput logical replication plugin's relation synchronization system. It tracks whether schema information has been sent to subscribers for specific relations and maintains the necessary state for row filtering, column lists, and partition handling. 

The structure handles complex scenarios in logical replication including streamed transactions where commit order may differ from send order, transaction aborts that require schema resending, and partition inheritance where changes may be published using an ancestor's schema. It optimizes replication by avoiding redundant schema transmissions while ensuring correctness across different transaction scenarios.

## Parameters / Member Variables
- : The OID of the relation this entry represents
- : Overall validity flag indicating if this cache entry is valid for replication
- : Boolean flag tracking whether the current schema has been sent to the subscriber
- : List of transaction IDs for streamed transactions that have already received this schema
- : Publication actions (insert/update/delete/truncate) configured for this relation
- : Array of expression states for row filtering, one per publication action type
- : Executor state context used for evaluating row filter expressions
- : Tuple table slot for storing new tuple values during replication
- : Tuple table slot for storing old tuple values during replication
- : OID of the relation to publish as (may differ from relid for partitions using ancestor schema)
- : Attribute mapping for converting tuples from partition schema to ancestor schema when needed
- : Bitmap of columns included in the publication (NULL means all columns)
- : Private memory context for storing additional state data for this entry

## Dependencies
- Functions called/Symbols referenced:
  - PublicationActions
  - NUM_ROWFILTER_PUBACTIONS
  - AttrMap
- Called from (representative examples):
  - maybe_send_schema
  - get_rel_sync_entry
  - pgoutput_row_filter
  - pgoutput_change
  - init_rel_sync_cache
  - cleanup_rel_sync_cache

## Notes and Other Information
- This structure is central to PostgreSQL's logical replication optimization, preventing redundant schema transmissions
- Handles complex partition inheritance scenarios where changes may be replicated using ancestor relation schemas
- Supports row filtering with separate expression states for different DML operations due to replica identity constraints
- Memory management uses a dedicated context (entry_cxt) for easy cleanup of associated resources
- The streamed_txns list is crucial for handling transaction ordering issues in streaming replication scenarios