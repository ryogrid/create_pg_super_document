# PublicationTable

## Location
[src/include/nodes/parsenodes.h:4131-4137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4131-L4137)

## Overview
PublicationTable represents a table specification within a logical replication publication, including the table reference, optional WHERE clause for row filtering, and optional column list for column filtering.

## Definition
```c
typedef struct PublicationTable
{
    NodeTag     type;
    RangeVar   *relation;      /* relation to be published */
    Node       *whereClause;   /* qualifications */
    List       *columns;       /* List of columns in a publication table */
} PublicationTable;
```

## Detailed Description
The PublicationTable structure is a parse node that represents a table specification within a PostgreSQL logical replication publication. Publications are used in logical replication to define which tables and what data from those tables should be replicated to subscribers. This structure encapsulates the table reference along with optional filtering criteria.

The structure supports advanced publication features including row-level filtering (via WHERE clause) and column-level filtering (via column list). Row filtering allows publications to include only rows that meet specific criteria, while column filtering allows publications to include only specific columns from a table. These features provide fine-grained control over what data is replicated.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a PublicationTable parse node
- `relation`: RangeVar pointer specifying the table to be included in the publication, potentially schema-qualified
- `whereClause`: Node pointer containing an optional WHERE clause expression for row-level filtering (NULL if no row filter is specified)
- `columns`: List of column names (String nodes) to include in the publication for column-level filtering (NULL if all columns should be published)

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar (structure representing table references with optional schema qualification)
  - NodeTag (for parse node identification)
  - Node (base node type for expression trees)
  - List (PostgreSQL's generic list structure)

- Called from (representative examples):
  - OpenTableList (used during publication table processing)
  - PublicationObjSpec (referenced in publication object specifications)

## Notes and Other Information
- PublicationTable is used within CREATE PUBLICATION and ALTER PUBLICATION statements to specify which tables to include
- The WHERE clause, if present, must be a valid boolean expression that can be evaluated for each row
- Row filtering is applied on the publisher side before data is sent to subscribers, reducing network traffic and storage requirements
- Column filtering allows selective replication of columns, which is useful for security (excluding sensitive columns) or optimization (excluding large columns not needed by subscribers)
- The relation field uses RangeVar to support both simple table names and schema-qualified names
- Part of PostgreSQL's logical replication system, enabling fine-grained control over what data is replicated
- Defined in the parsenodes.h header file as part of the SQL parsing infrastructure