# PublicationObjSpec

## Location
[src/include/nodes/parsenodes.h:4151-4158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4151-L4158)

## Overview
A structure that represents a specification for objects that can be included in a PostgreSQL publication, supporting various types of publishable objects including individual tables, all tables in a schema, or continuation objects.

## Definition

```c
typedef struct PublicationObjSpec
{
	NodeTag		type;
	PublicationObjSpecType pubobjtype;	/* type of this publication object */
	char	   *name;
	PublicationTable *pubtable;
	ParseLoc	location;		/* token location, or -1 if unknown */
} PublicationObjSpec;
```
## Detailed Description
PublicationObjSpec is a parse tree node structure used during SQL parsing to represent different types of objects that can be included in a publication. It supports four main types of publication objects through the PublicationObjSpecType enum:

- : Represents a specific table to be published
- : Represents all tables within a specified schema
- : Represents all tables in the first element of search_path
- : Used for continuation of previous type specifications

This structure is primarily used during the parsing phase of CREATE PUBLICATION and ALTER PUBLICATION statements to capture the user's intent regarding what database objects should be included in the publication.

## Parameters / Member Variables
- : Standard NodeTag for identifying the node type in PostgreSQL's parse tree system
- : Enum value specifying the type of publication object (table, schema tables, etc.)
- : String name of the object (table name, schema name, etc.), can be NULL for certain types
- : Pointer to PublicationTable structure containing detailed table specification including relation, WHERE clause, and column list; used when pubobjtype is PUBLICATIONOBJ_TABLE
- : Parse location information for error reporting, set to -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - [PublicationObjSpecType](PublicationObjSpecType.md) (enum defining object types)
  - [PublicationTable](PublicationTable.md) (struct for table-specific publication details)
  - ParseLoc (location tracking for parser)
  - NodeTag (standard parse tree node identifier)
- Called from (representative examples):
  - [ObjectsInPublicationToOids](../O/ObjectsInPublicationToOids.md) (in publicationcmds.c for converting specs to OIDs)

## Notes and Other Information
- This structure is part of PostgreSQL's logical replication system, introduced to support selective publication of database objects
- The structure allows for flexible specification of publication contents, from individual tables to entire schemas
- The location field is particularly important for providing accurate error messages during SQL parsing
- When pubobjtype is not PUBLICATIONOBJ_TABLE, the pubtable field is typically NULL
- The CONTINUATION type is used internally for handling complex multi-part specifications