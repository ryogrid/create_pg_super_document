# RelationInfo

## Location
src/bin/pg_amcheck/pg_amcheck.c: 156 - 166

## Overview
RelationInfo is a structure used in PostgreSQL's pg_amcheck utility to store detailed information about a specific relation (table or index) that needs integrity checking, including metadata and runtime state.

## Definition
```c
typedef struct RelationInfo
{
    const DatabaseInfo *datinfo;    /* shared by other relinfos */
    Oid         reloid;
    bool        is_heap;            /* true if heap, false if btree */
    char       *nspname;
    char       *relname;
    int         relpages;
    int         blocks_to_check;
    char       *sql;                /* set during query run, pg_free'd after */
} RelationInfo;
```

## Detailed Description
The RelationInfo structure represents a complete context for checking a specific database relation within pg_amcheck. It maintains both static metadata about the relation (OID, name, type, size) and runtime information (SQL commands, blocks to check). The structure includes a reference to the containing DatabaseInfo, establishing the hierarchical relationship between databases and their relations. The is_heap flag distinguishes between heap tables and btree indexes, enabling type-specific checking logic. The sql field is dynamically allocated during query execution and freed afterward, supporting efficient memory management during batch operations.

## Parameters / Member Variables
- `datinfo`: Pointer to the DatabaseInfo structure containing this relation, shared among multiple RelationInfo instances
- `reloid`: Object identifier (OID) of the relation in PostgreSQL's system catalogs
- `is_heap`: Boolean flag indicating relation type (true for heap tables, false for btree indexes)
- `nspname`: Schema (namespace) name containing this relation
- `relname`: Name of the relation (table or index)
- `relpages`: Total number of pages in the relation according to system catalogs
- `blocks_to_check`: Number of blocks that will actually be checked (may be less than relpages due to filtering)
- `sql`: Dynamically allocated SQL command string used during execution, freed after use

## Dependencies
- Functions called/Symbols referenced:
  - DatabaseInfo (referenced by datinfo member)
- Called from (representative examples):
  - prepare_heap_command
  - prepare_btree_command
  - verify_heap_slot_handler
  - verify_btree_slot_handler
  - compile_relation_list_one_db
  - main (in pg_amcheck)

## Notes and Other Information
- Defined in src/bin/pg_amcheck/pg_amcheck.c:156-166
- Represents individual relations (tables/indexes) within the pg_amcheck checking framework
- The datinfo pointer is shared among multiple RelationInfo instances from the same database for memory efficiency
- Supports both heap tables and btree indexes with type-specific handling via the is_heap flag
- The blocks_to_check field allows for partial checking scenarios (e.g., startblock/endblock ranges)
- SQL field management follows a pattern of allocation during use and immediate cleanup
- Part of the hierarchical organization: DatabaseInfo contains multiple RelationInfo instances