# PUBACTION_DELETE

## Location
[src/backend/replication/pgoutput/pgoutput.c:109-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L109-L111)

## Overview
PUBACTION_DELETE is an enum value representing the delete operation in PostgreSQL's logical replication row filtering system, used to identify and filter DELETE operations during publication.

## Definition

```c
PUBACTION_UPDATE,
	PUBACTION_DELETE,
};

#define NUM_ROWFILTER_PUBACTIONS (PUBACTION_DELETE+1)

/*
 * Entry in the map used to remember which relation schemas we sent.
 *
 * The schema_sent flag determines if the current schema record for the
 * relation (and for its ancestor if publish_as_relid is set) was already
 * sent to the subscriber (in which case we don't need to send it again).
 *
 * The schema cache on downstream is however updated only at commit time,
 * and with streamed transactions the commit order may be different from
 * the order the transactions are sent in. Also, the (sub) transactions
 * might get aborted so we need to send the schema for each (sub) transaction
 * so that we don't lose the schema information on abort. For handling this,
 * we maintain the list of xids (streamed_txns) for those we have already sent
 * the schema.
 *
 * For partitions, 'pubactions' considers not only the table's own
 * publications, but also those of all of its ancestors.
 */
typedef struct RelationSyncEntry
```
## Detailed Description
PUBACTION_DELETE is the third and final member of the RowFilterPubAction enum, defined in the pgoutput logical replication plugin. This enum value is specifically used to represent DELETE operations when implementing row-level filtering for logical replication publications. The enum serves as an index into various arrays and data structures that manage row filtering logic for different DML operations.

The enum is part of PostgreSQL's logical replication infrastructure, where it helps categorize and process different types of database modification operations. Only three publication actions are used for row filtering (insert, update, delete), making this a complete enumeration of the supported DML operations that can be filtered in logical replication.

## Parameters / Member Variables
- This is an enum constant with no parameters or member variables
- Enum value: 2 (third member of RowFilterPubAction enum)

## Dependencies
- Functions called/Symbols referenced:
  - None (enum constant)
- Called from (representative examples):
  - NUM_ROWFILTER_PUBACTIONS (macro calculation)
  - [pgoutput_row_filter_init](../p/pgoutput_row_filter_init.md) (row filter initialization)
  - [pgoutput_row_filter](../p/pgoutput_row_filter.md) (row filtering logic)
  - map_changetype_pubaction array (change type mapping)

## Notes and Other Information
- Used as an array index in row filtering data structures, requiring careful bounds checking
- Part of a mapping system that converts REORDER_BUFFER_CHANGE_DELETE to PUBACTION_DELETE
- The enum value is used to index into various arrays including no_filter[], rfnodes[], and map_changetype_pubaction[]
- Critical for logical replication row filtering where DELETE operations need to be identified and potentially filtered based on publication settings
- The NUM_ROWFILTER_PUBACTIONS macro uses this value (+1) to define the total number of supported publication actions
- Located in src/backend/replication/pgoutput/pgoutput.c:109