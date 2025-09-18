# ReindexObjectType

## Location
[src/include/nodes/parsenodes.h:3972-3973](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3972-L3973)

## Overview
ReindexObjectType is an enumeration that specifies the type of database object to be reindexed in PostgreSQL's REINDEX statement.

## Definition


## Detailed Description
This enumeration defines the different types of database objects that can be targeted by PostgreSQL's REINDEX command. The REINDEX operation rebuilds indexes to optimize performance, remove index bloat, or recover from index corruption. Each enum value corresponds to a different scope of reindexing operation, from individual indexes to entire databases.

## Parameters / Member Variables
- : Reindex a specific index object
- : Reindex all indexes of a table or materialized view
- : Reindex all indexes within a schema
- : Reindex system catalog indexes
- : Reindex all indexes in the current database

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - ReindexStmt (as the 'kind' field)
  - [ReindexMultipleTables](ReindexMultipleTables.md) function
  - Parser grammar rules in gram.y

## Notes and Other Information
- This enum is part of the parse node structures used in PostgreSQL's SQL parser
- The enum values are used in the ReindexStmt structure to specify what type of object should be reindexed
- The parser maps SQL keywords (INDEX, TABLE, SCHEMA, SYSTEM, DATABASE) to these enum values
- Located in src/include/nodes/parsenodes.h, making it part of the core parsing infrastructure