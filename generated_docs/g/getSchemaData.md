# getSchemaData

## Location
src/bin/pg_dump/common.c: 99 - 292

## Overview
Orchestrates the collection of all potentially dumpable database objects from a PostgreSQL database for pg_dump operations.

## Definition


## Detailed Description
The getSchemaData function is the central orchestrator in pg_dump that systematically collects metadata about all database objects that might need to be dumped. It coordinates the reading of various PostgreSQL system catalogs in a specific order to ensure proper dependency resolution and relationship establishment. The function ensures extensions are processed first since extension membership affects dumping decisions for other objects, followed by namespaces and tables which form the foundation for most other objects.

The function carefully sequences the collection process to respect dependencies between different object types. For example, types must be read after tables and functions since they may depend on them, and inheritance relationships are processed after all basic object information is gathered to properly link parent-child relationships.

## Parameters / Member Variables
- : Archive structure containing database connection and dump configuration information
- : Output parameter that receives the count of tables found in the database

## Dependencies
- Functions called/Symbols referenced:
  - getExtensions (reads extension information first)
  - getExtensionMembership (identifies extension members)
  - getNamespaces (reads schema information)
  - getTables (reads table metadata)
  - getTypes (reads user-defined types)
  - getFuncs (reads user-defined functions)
  - flagInhTables (processes inheritance relationships)
  - flagInhAttrs (flags inherited columns)
  - flagInhIndexes (flags inherited indexes)
  - getIndexes, getConstraints, getTriggers (reads table-related objects)
  - getPublications, getSubscriptions (reads logical replication objects)
- Called from (representative examples):
  - main (src/bin/pg_dump/pg_dump.c:956)

## Notes and Other Information
The function follows a strict ordering to ensure proper dependency resolution:
1. Extensions first (affects dumping decisions for other objects)
2. Namespaces second (tables need to link to their schemas)
3. Tables early (minimizes lock acquisition window)
4. Functions and types (may have interdependencies)
5. Inheritance processing after basic objects are loaded
6. Indexes and constraints after table structure is complete

The function returns a TableInfo array which serves as the primary data structure for subsequent dump operations, containing not only table information but also serving as an anchor point for related objects.