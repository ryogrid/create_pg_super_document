# RelationSyncEntry

## Location
[src/backend/replication/pgoutput/pgoutput.c:132-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L132-L185)

## Overview
RelationSyncEntry is a struct used in PostgreSQL's logical replication pgoutput plugin to cache schema information and publication metadata for relations being replicated to subscribers.

## Definition

```c
typedef struct RelationSyncEntry
{
	Oid			relid;			/* relation oid */

	bool		replicate_valid;	/* overall validity flag for entry */

	bool		schema_sent;
	List	   *streamed_txns;	/* streamed toplevel transactions with this
								 * schema */

	/* are we publishing this rel? */
	PublicationActions pubactions;

	/*
	 * ExprState array for row filter. Different publication actions don't
	 * allow multiple expressions to always be combined into one, because
	 * updates or deletes restrict the column in expression to be part of the
	 * replica identity index whereas inserts do not have this restriction, so
	 * there is one ExprState per publication action.
	 */
	ExprState  *exprstate[NUM_ROWFILTER_PUBACTIONS];
	EState	   *estate;			/* executor state used for row filter */
	TupleTableSlot *new_slot;	/* slot for storing new tuple */
	TupleTableSlot *old_slot;	/* slot for storing old tuple */

	/*
	 * OID of the relation to publish changes as.  For a partition, this may
	 * be set to one of its ancestors whose schema will be used when
	 * replicating changes, if publish_via_partition_root is set for the
	 * publication.
	 */
	Oid			publish_as_relid;

	/*
	 * Map used when replicating using an ancestor's schema to convert tuples
	 * from partition's type to the ancestor's; NULL if publish_as_relid is
	 * same as 'relid' or if unnecessary due to partition and the ancestor
	 * having identical TupleDesc.
	 */
	AttrMap    *attrmap;

	/*
	 * Columns included in the publication, or NULL if all columns are
	 * included implicitly.  Note that the attnums in this bitmap are not
	 * shifted by FirstLowInvalidHeapAttributeNumber.
	 */
	Bitmapset  *columns;

	/*
	 * Private context to store additional data for this entry - state for the
	 * row filter expressions, column list, etc.
	 */
	MemoryContext entry_cxt;
} RelationSyncEntry;
```
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
  - [PublicationActions](../P/PublicationActions.md)
  - NUM_ROWFILTER_PUBACTIONS
  - [AttrMap](../A/AttrMap.md)
- Called from (representative examples):
  - [maybe_send_schema](../m/maybe_send_schema.md)
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)
  - [pgoutput_row_filter](../p/pgoutput_row_filter.md)
  - [pgoutput_change](../p/pgoutput_change.md)
  - [init_rel_sync_cache](../i/init_rel_sync_cache.md)
  - [cleanup_rel_sync_cache](../c/cleanup_rel_sync_cache.md)

## Notes and Other Information
- This structure is central to PostgreSQL's logical replication optimization, preventing redundant schema transmissions
- Handles complex partition inheritance scenarios where changes may be replicated using ancestor relation schemas
- Supports row filtering with separate expression states for different DML operations due to replica identity constraints
- Memory management uses a dedicated context (entry_cxt) for easy cleanup of associated resources
- The streamed_txns list is crucial for handling transaction ordering issues in streaming replication scenarios