# is_publishable_class

## Location
[src/backend/catalog/pg_publication.c:137-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L137-L149)

## Overview
A static utility function that determines if a relation is publishable based on its OID and pg_class form tuple, without requiring the relation to be opened.

## Definition
```c
static bool is_publishable_class(Oid relid, Form_pg_class reltuple)
```

## Detailed Description
This function performs the same validation checks as `check_publication_add_relation()` but is designed for efficiency - it doesn't require the relation to be opened and returns a boolean result instead of throwing errors. It determines publishability based on four criteria:

1. **Relation Kind**: Only regular tables (RELKIND_RELATION) and partitioned tables (RELKIND_PARTITIONED_TABLE) are publishable
2. **System Relations**: Catalog relations are not publishable (checked via IsCatalogRelationOid)
3. **Persistence**: Only permanent tables are publishable (RELPERSISTENCE_PERMANENT) - excludes temporary and unlogged tables
4. **Object Age**: Only relations created after initdb are publishable (relid >= FirstNormalObjectId)

The function includes a notable design consideration: it excludes all tables created during initdb (including information_schema tables) via the FirstNormalObjectId check. The extensive comment notes this is somewhat redundant with IsCatalogRelationOid() but serves to exclude information_schema tables, though this approach has limitations.

## Parameters / Member Variables
- `relid`: OID of the relation to check for publishability
- `reltuple`: Form_pg_class tuple containing the relation's metadata from pg_class

## Dependencies
- Functions called/Symbols referenced:
  - [IsCatalogRelationOid](../I/IsCatalogRelationOid.md)
- Constants referenced:
  - RELKIND_RELATION
  - RELKIND_PARTITIONED_TABLE
  - RELPERSISTENCE_PERMANENT
  - FirstNormalObjectId
- Types used:
  - Form_pg_class
- Called from:
  - [is_publishable_relation](is_publishable_relation.md)
  - [pg_relation_is_publishable](../p/pg_relation_is_publishable.md)
  - [GetAllTablesPublicationRelations](../G/GetAllTablesPublicationRelations.md)
  - [GetSchemaPublicationRelations](../G/GetSchemaPublicationRelations.md)

## Notes and Other Information
- This is a static function, only accessible within pg_publication.c
- Designed for performance - avoids opening relations and doesn't throw errors
- The function includes extensive commentary about design trade-offs and future improvement possibilities
- The developers note that a future `relispublishable` column in pg_class might be a better long-term solution
- The FirstNormalObjectId check is considered somewhat inadequate as information_schema could be dropped and recreated
- Returns a simple boolean rather than generating detailed error messages like the check_* functions
- Location: src/backend/catalog/pg_publication.c:137-149

## Simplified Source

```c
static bool is_publishable_class(Oid relid, Form_pg_class reltuple) {
    // Check all publishability criteria:
    // 1. Must be a regular table or partitioned table
    // 2. Must not be a catalog relation
    // 3. Must be a permanent table (not temporary/unlogged)
    // 4. Must be created after initdb (user-created)
    return (reltuple->relkind == RELKIND_RELATION ||
            reltuple->relkind == RELKIND_PARTITIONED_TABLE) &&
           !IsCatalogRelationOid(relid) &&
           reltuple->relpersistence == RELPERSISTENCE_PERMANENT &&
           relid >= FirstNormalObjectId;
}
```