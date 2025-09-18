# DefineRelation

## Location
[src/backend/commands/tablecmds.c:698-1290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L698-L1290)

## Overview
DefineRelation is the core function for creating new relations (tables, views, indexes, etc.) in PostgreSQL, handling the complete process from parsing CREATE statements to catalog registration.

## Definition


## Detailed Description
DefineRelation serves as the primary entry point for creating database relations in PostgreSQL. It processes CREATE TABLE statements and related commands, coordinating the entire relation creation workflow. The function handles schema validation, inheritance processing, constraint management, partitioning setup, and catalog registration. It operates by first validating the creation parameters, processing inheritance relationships, building the relation descriptor, creating the physical relation through heap_create_with_catalog, and finally setting up any additional features like partitioning, indexes, and constraints.

## Parameters / Member Variables
- : CreateStmt parse tree containing the parsed CREATE TABLE statement with all table definition elements
- : Character indicating the relation type (RELKIND_RELATION for tables, RELKIND_VIEW for views, etc.)
- : Object identifier of the relation owner, or InvalidOid to use current user
- : Optional output parameter to receive the address of the corresponding pg_type entry
- : Original SQL query string used for error reporting and context

## Dependencies
- Functions called/Symbols referenced:
  - [BuildDescForRelation](../B/BuildDescForRelation.md)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - [MergeAttributes](../M/MergeAttributes.md)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [transformRelOptions](../t/transformRelOptions.md)
  - [view_reloptions](../v/view_reloptions.md)
  - [partitioned_table_reloptions](../p/partitioned_table_reloptions.md)
  - [heap_reloptions](../h/heap_reloptions.md)
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md)
  - [StorePartitionBound](../S/StorePartitionBound.md)
  - [StoreCatalogInheritance](../S/StoreCatalogInheritance.md)
  - [relation_open](../r/relation_open.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)
  - [DefineSequence](DefineSequence.md)
  - [DefineCompositeType](DefineCompositeType.md)
  - [DefineVirtualRelation](DefineVirtualRelation.md)
  - [create_ctas_internal](../c/create_ctas_internal.md)

## Notes and Other Information
DefineRelation is a complex function spanning nearly 600 lines that orchestrates the entire relation creation process. It performs extensive validation including permission checks, tablespace verification, and inheritance consistency. The function handles special cases for partitioned tables, temporary tables, and security-restricted operations. It processes both raw and cooked defaults/constraints, with raw expressions requiring later transformation after the relation exists. For partitioned tables, it sets up partition keys and validates partition bounds. When creating partitions, it automatically inherits indexes, triggers, and foreign key constraints from the parent table.