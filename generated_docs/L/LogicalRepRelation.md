# LogicalRepRelation

## Location
[src/include/replication/logicalproto.h:104-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/logicalproto.h#L104-L116)

## Overview
LogicalRepRelation is a structure that contains comprehensive metadata about a relation (table) in logical replication, storing information from the remote side to enable proper replication handling.

## Definition

```c
typedef struct LogicalRepRelation
{
	/* Info coming from the remote side. */
	LogicalRepRelId remoteid;	/* unique id of the relation */
	char	   *nspname;		/* schema name */
	char	   *relname;		/* relation name */
	int			natts;			/* number of columns */
	char	  **attnames;		/* column names */
	Oid		   *atttyps;		/* column types */
	char		replident;		/* replica identity */
	char		relkind;		/* remote relation kind */
	Bitmapset  *attkeys;		/* Bitmap of key columns */
} LogicalRepRelation;
```
## Detailed Description
This structure serves as a comprehensive metadata container for relations participating in logical replication. It captures all essential information about a remote table that is necessary for the local subscriber to properly interpret and apply replicated changes. The structure includes both basic table information (name, schema) and detailed column metadata (names, types, key columns).

The structure is particularly important for handling schema differences between publisher and subscriber, as it provides the remote table's structure which may differ from the local table. The replica identity and key column information are crucial for properly identifying and updating the correct rows during replication operations.

## Parameters / Member Variables
- `remoteid`: Unique identifier for the relation on the remote (publisher) side, of type LogicalRepRelId
- `*nspname`: String containing the schema (namespace) name of the remote relation
- `*relname`: String containing the name of the remote relation
- `natts`: Integer specifying the number of columns in the remote relation
- `**attnames`: Array of strings containing the names of all columns in the remote relation
- `*atttyps`: Array of Oid values representing the data types of each column
- `replident`: Character indicating the replica identity setting for the relation
- `relkind`: Character indicating the kind of relation (table, view, etc.) on the remote side
- `*attkeys`: Bitmapset indicating which columns are part of the key used for identifying rows
## Dependencies
- Functions called/Symbols referenced:
  - LogicalRepRelId
  - Oid
  - [Bitmapset](../B/Bitmapset.md)
- Called from (representative examples):
  - [logicalrep_write_rel](../l/logicalrep_write_rel.md)
  - [logicalrep_read_rel](../l/logicalrep_read_rel.md)
  - logicalrep_read_attrs
  - logicalrep_relmap_update
  - logicalrep_rel_open
  - [apply_handle_relation](../a/apply_handle_relation.md)
  - [fetch_remote_table_info](../f/fetch_remote_table_info.md)
  - [copy_table](../c/copy_table.md)

## Notes and Other Information
- This structure represents the remote table's metadata, which may differ from the local table's structure
- The replica identity setting determines which columns are used to identify rows for UPDATE and DELETE operations
- Memory management for the string arrays and bitmapset must be handled properly to avoid leaks
- The structure is used extensively in relation mapping and tuple routing operations
- Key columns (attkeys) are essential for properly identifying rows when applying changes
- Located in src/include/replication/logicalproto.h:104-116