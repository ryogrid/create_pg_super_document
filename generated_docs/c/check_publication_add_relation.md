# check_publication_add_relation

## Location
[src/backend/catalog/pg_publication.c:59-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L59-L97)

## Overview
A static validation function that checks if a relation (table) can be added to a publication, throwing appropriate errors if the relation is not suitable for publication.

## Definition

```c
static void
check_publication_add_relation(Relation targetrel)
```
## Detailed Description
This function performs validation checks to ensure that a given relation can be safely added to a logical replication publication. It enforces several restrictions:

1. **Relation Kind Validation**: Only regular tables (RELKIND_RELATION) and partitioned tables (RELKIND_PARTITIONED_TABLE) are allowed in publications.

2. **System Table Restriction**: System/catalog tables cannot be added to publications as they are not suitable for logical replication.

3. **Persistence Validation**: Tables with certain persistence characteristics are forbidden:
   - Temporary tables (RELPERSISTENCE_TEMP) cannot be published
   - Unlogged tables (RELPERSISTENCE_UNLOGGED) cannot be published

The function uses PostgreSQL's error reporting mechanism to provide detailed error messages when validation fails, including specific reasons why the relation cannot be published.

## Parameters / Member Variables
- `targetrel`: A Relation pointer to the table being validated for publication inclusion
## Dependencies
- Functions called/Symbols referenced:
  - RelationGetForm
  - RelationGetRelationName  
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [IsCatalogRelation](../I/IsCatalogRelation.md)
  - ereport (error reporting)
- Constants referenced:
  - RELKIND_RELATION
  - RELKIND_PARTITIONED_TABLE
  - RELPERSISTENCE_TEMP
  - RELPERSISTENCE_UNLOGGED
- Called from:
  - [publication_add_relation](../p/publication_add_relation.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_publication.c compilation unit
- The function only performs validation and does not modify any state
- Error messages are standardized and include both the relation name and specific reason for rejection
- The function is designed to fail fast - it stops at the first validation error encountered
- Location: src/backend/catalog/pg_publication.c:59-97

## Simplified Source

```c
static void
check_publication_add_relation(Relation targetrel)
{
    // Only regular and partitioned tables allowed
    if (RelationGetForm(targetrel)->relkind != RELKIND_RELATION &&
        RelationGetForm(targetrel)->relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot add relation \"%s\" to publication",
                        RelationGetRelationName(targetrel)),
                 errdetail_relkind_not_supported(RelationGetForm(targetrel)->relkind)));

    // System/catalog tables cannot be published
    if (IsCatalogRelation(targetrel))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot add relation \"%s\" to publication",
                        RelationGetRelationName(targetrel)),
                 errdetail("This operation is not supported for system tables.")));

    // Check table persistence - temp and unlogged tables not allowed
    if (targetrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot add relation \"%s\" to publication",
                        RelationGetRelationName(targetrel)),
                 errdetail("This operation is not supported for temporary tables.")));
    else if (targetrel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot add relation \"%s\" to publication",
                        RelationGetRelationName(targetrel)),
                 errdetail("This operation is not supported for unlogged tables.")));
}
```