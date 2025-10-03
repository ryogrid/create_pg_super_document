# create_ctas_internal

## Location
[src/backend/commands/createas.c:80-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L80-L152)

## Overview
Internal utility function used for creating the physical relation definition for both CREATE TABLE AS statements and materialized views in PostgreSQL.

## Definition

```c
static ObjectAddress
create_ctas_internal(List *attrList, IntoClause *into)
```
## Detailed Description
The  function serves as a core utility for implementing CREATE TABLE AS and CREATE MATERIALIZED VIEW operations. It constructs the physical relation by creating a synthetic  node and delegating to  for the actual table creation. The function handles the setup of table attributes, relation options, and determines the appropriate relation kind (regular table or materialized view) based on the presence of a view query in the .

After creating the base relation, the function manages TOAST table creation when necessary and handles the view definition storage for materialized views. The function ensures proper command counter increments for visibility of created objects.

## Parameters / Member Variables
- `*attrList`: List of  nodes representing the column definitions for the new relation
- `*into`:  containing target relation information, options, and optional view query for materialized views
## Dependencies
- Functions called/Symbols referenced:
  - [DefineRelation](../D/DefineRelation.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [transformRelOptions](../t/transformRelOptions.md)
  - [heap_reloptions](../h/heap_reloptions.md)
  - [NewRelationCreateToastTable](../N/NewRelationCreateToastTable.md)
  - copyObject
  - [StoreViewQuery](../S/StoreViewQuery.md)
- Called from (representative examples):
  - DR_intorel
  - [create_ctas_nodata](create_ctas_nodata.md)
  - [intorel_startup](../i/intorel_startup.md)

## Notes and Other Information
- This is a static function within the createas.c file, serving as an internal implementation detail
- Supports both regular tables (RELKIND_RELATION) and materialized views (RELKIND_MATVIEW)
- Automatically creates TOAST tables when necessary for the target relation
- For materialized views, stores the view query definition after creating the physical relation
- Uses command counter increments strategically to ensure object visibility during the creation process

## Simplified Source

```c
static ObjectAddress create_ctas_internal(List *attrList, IntoClause *into)
{
    CreateStmt *create = makeNode(CreateStmt);
    bool is_matview;
    char relkind;
    Datum toast_options;
    static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
    ObjectAddress intoRelationAddr;

    // Determine if this is a materialized view or regular table
    is_matview = (into->viewQuery != NULL);
    relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

    // Build a synthetic CREATE TABLE statement
    create->relation = into->rel;
    create->tableElts = attrList;
    create->inhRelations = NIL;
    create->ofTypename = NULL;
    create->constraints = NIL;
    create->options = into->options;
    create->oncommit = into->onCommit;
    create->tablespacename = into->tableSpaceName;
    create->if_not_exists = false;
    create->accessMethod = into->accessMethod;

    // Create the physical relation
    intoRelationAddr = DefineRelation(create, relkind, InvalidOid, NULL, NULL);

    // Ensure the relation is visible for TOAST table creation
    CommandCounterIncrement();

    // Set up TOAST table if needed
    toast_options = transformRelOptions((Datum) 0, create->options, "toast",
                                      validnsps, true, false);
    (void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
    NewRelationCreateToastTable(intoRelationAddr.objectId, toast_options);

    // For materialized views, store the view definition
    if (is_matview) {
        Query *query = (Query *) copyObject(into->viewQuery);
        StoreViewQuery(intoRelationAddr.objectId, query, false);
        CommandCounterIncrement();
    }

    return intoRelationAddr;
}
```