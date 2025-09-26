# CreatePublicationStmt

## Location
[src/include/nodes/parsenodes.h:4160-4167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4160-L4167)

## Overview
A parse tree node structure representing a CREATE PUBLICATION SQL statement, which defines a new publication for logical replication in PostgreSQL.

## Definition

```c
typedef struct CreatePublicationStmt
{
	NodeTag		type;
	char	   *pubname;		/* Name of the publication */
	List	   *options;		/* List of DefElem nodes */
	List	   *pubobjects;		/* Optional list of publication objects */
	bool		for_all_tables; /* Special publication for all tables in db */
} CreatePublicationStmt;
```
## Detailed Description
CreatePublicationStmt represents the parsed form of a CREATE PUBLICATION statement used in PostgreSQL's logical replication system. This structure captures all the components needed to create a new publication, which serves as a mechanism to selectively replicate data changes from specific database objects to subscribers.

The structure supports two main modes of operation:
1. **Selective publication**: When  is false, the publication includes only the objects specified in 
2. **All-tables publication**: When  is true, the publication automatically includes all current and future tables in the database

Publications can be configured with various options such as whether to replicate INSERT, UPDATE, DELETE operations, and can include WHERE clauses and column lists for fine-grained control over what data gets replicated.

## Parameters / Member Variables
- : Standard NodeTag identifier for the parse tree node system
- : String containing the name of the publication being created; this name must be unique within the database
- : List of DefElem nodes representing publication options such as 'publish' (specifying which DML operations to replicate), 'publish_via_partition_root', etc.
- : Optional list of PublicationObjSpec nodes defining which database objects (tables, schemas) to include in the publication; NULL when creating an all-tables publication
- : Boolean flag indicating whether this is a special publication that includes all tables in the database, bypassing the need for explicit object specification

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (standard parse tree node identifier)
  - [List](../L/List.md) (PostgreSQL's generic list structure)
  - [DefElem](../D/DefElem.md) (definition element for options)
  - [PublicationObjSpec](../P/PublicationObjSpec.md) (implicit, through pubobjects list)
- Called from (representative examples):
  - [CreatePublication](CreatePublication.md) (in publicationcmds.c for statement execution)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (in utility.c for statement processing)

## Notes and Other Information
- This structure is created during SQL parsing and consumed during statement execution
- The  option creates a publication that automatically includes all existing tables and any tables created in the future
- [Publication](../P/Publication.md) options are extensible and can include parameters like 'publish' (INSERT, UPDATE, DELETE, TRUNCATE), 'publish_via_partition_root'
- When  is specified, it contains a mix of table specifications, schema specifications, or continuation markers
- Publications are a key component of PostgreSQL's logical replication architecture, working in conjunction with subscriptions
- The statement requires appropriate privileges (CREATE privilege on database or superuser)