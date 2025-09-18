# create_ctas_internal

## Location
src/backend/commands/createas.c: 80 - 152

## Overview
Internal utility function used for creating the physical relation definition for both CREATE TABLE AS statements and materialized views in PostgreSQL.

## Definition


## Detailed Description
The  function serves as a core utility for implementing CREATE TABLE AS and CREATE MATERIALIZED VIEW operations. It constructs the physical relation by creating a synthetic  node and delegating to  for the actual table creation. The function handles the setup of table attributes, relation options, and determines the appropriate relation kind (regular table or materialized view) based on the presence of a view query in the .

After creating the base relation, the function manages TOAST table creation when necessary and handles the view definition storage for materialized views. The function ensures proper command counter increments for visibility of created objects.

## Parameters / Member Variables
- : List of  nodes representing the column definitions for the new relation
- :  containing target relation information, options, and optional view query for materialized views

## Dependencies
- Functions called/Symbols referenced:
  - [DefineRelation](../D/DefineRelation.md)
  - CommandCounterIncrement
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