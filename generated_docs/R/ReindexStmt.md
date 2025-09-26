# ReindexStmt

## Location
[src/include/nodes/parsenodes.h:3974-3982](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3974-L3982)

## Overview
ReindexStmt represents a REINDEX statement in PostgreSQL's parse tree, which is used to rebuild indexes to recover from index corruption or improve performance.

## Definition
```c
typedef struct ReindexStmt
{
    NodeTag             type;
    ReindexObjectType   kind;      /* REINDEX_OBJECT_INDEX, REINDEX_OBJECT_TABLE, etc. */
    RangeVar           *relation;  /* Table or index to reindex */
    const char         *name;      /* name of database to reindex */
    List               *params;    /* list of DefElem nodes */
} ReindexStmt;
```

## Detailed Description
ReindexStmt is a parse tree node that represents the REINDEX SQL command. The REINDEX statement rebuilds one or more indexes, which can be necessary to recover from index corruption, reclaim disk space after bulk deletions, or improve performance by eliminating index bloat.

The statement supports reindexing at different granularities: individual indexes, entire tables (including all their indexes), schemas, system catalogs, or entire databases. It also supports various options like CONCURRENTLY (to avoid blocking concurrent operations) and VERBOSE (for detailed progress reporting).

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a ReindexStmt node in the parse tree
- `kind`: ReindexObjectType enum specifying the scope of reindexing (INDEX, TABLE, SCHEMA, SYSTEM, or DATABASE)
- `relation`: RangeVar structure representing the specific table or index to reindex (NULL for SCHEMA, SYSTEM, or DATABASE)
- `name`: String containing the database name for DATABASE-level reindexing (NULL otherwise)
- `params`: List of DefElem nodes containing options like "concurrently", "verbose", or "tablespace"

## Dependencies
- Functions called/Symbols referenced:
  - ReindexObjectType (enum defining reindex scope)
  - RangeVar (for relation references)
  - DefElem (for parameter options)
  
- Called from (representative examples):
  - ExecReindex (main execution function in indexcmds.c:2693)
  - ReindexIndex (single index reindexing in indexcmds.c:2788)
  - ReindexTable (table reindexing in indexcmds.c:2918)
  - ReindexMultipleTables (multiple table reindexing in indexcmds.c:2977)
  - ProcessUtilitySlow (utility command processor in utility.c:1567)

## Notes and Other Information
- The CONCURRENTLY option allows non-blocking reindex operations but has limitations on certain index types
- REINDEX SYSTEM affects only system catalog indexes and requires restricted access
- REINDEX DATABASE cannot be run inside a transaction block
- The params list supports options: verbose, concurrently, tablespace
- Different object types require different privileges and have different restrictions
- The operation can be resource-intensive and may require significant disk space during execution
- Used for index maintenance, corruption recovery, and performance optimization