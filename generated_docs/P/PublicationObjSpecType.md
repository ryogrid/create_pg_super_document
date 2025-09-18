# PublicationObjSpecType

## Location
[src/include/nodes/parsenodes.h:4149-4150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4149-L4150)

## Overview
PublicationObjSpecType is an enumeration that specifies the type of database object to be included in a PostgreSQL logical replication publication.

## Definition


## Detailed Description
This enumeration defines the different types of objects that can be specified when creating or altering a PostgreSQL publication for logical replication. Publications are used to define a set of tables whose data changes will be replicated to subscribers. The enum provides flexibility in specifying individual tables, all tables in a schema, or using continuation semantics for complex publication definitions.

## Parameters / Member Variables
- : Specifies a single table to be included in the publication
- : Includes all tables within a specified schema
- : Includes all tables in the current schema (first element of search_path)
- : Used for parsing continuation of previous object type specifications

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - PublicationObjSpec (as the 'pubobjtype' field)
  - Parser grammar rules in gram.y for CREATE/ALTER PUBLICATION statements

## Notes and Other Information
- This enum is part of PostgreSQL's logical replication infrastructure
- Used in CREATE PUBLICATION and ALTER PUBLICATION statements
- The PUBLICATIONOBJ_CONTINUATION value is a parsing artifact used to handle complex multi-object publication specifications
- Works in conjunction with PublicationObjSpec structure to represent parsed publication object specifications
- Publications are a key component of PostgreSQL's built-in logical replication feature introduced in version 10
- Located in src/include/nodes/parsenodes.h as part of the SQL parsing framework