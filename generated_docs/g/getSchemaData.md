# getSchemaData

## Location
[src/bin/pg_dump/common.c:99-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L99-L292)

## Overview
Orchestrates the collection of all potentially dumpable database objects from a PostgreSQL database for pg_dump operations.

## Definition

```c
TableInfo *
getSchemaData(Archive *fout, int *numTablesPtr)
```
## Detailed Description
The getSchemaData function is the central orchestrator in pg_dump that systematically collects metadata about all database objects that might need to be dumped. It coordinates the reading of various PostgreSQL system catalogs in a specific order to ensure proper dependency resolution and relationship establishment. The function ensures extensions are processed first since extension membership affects dumping decisions for other objects, followed by namespaces and tables which form the foundation for most other objects.

The function carefully sequences the collection process to respect dependencies between different object types. For example, types must be read after tables and functions since they may depend on them, and inheritance relationships are processed after all basic object information is gathered to properly link parent-child relationships.

## Parameters / Member Variables
- `*fout`: Archive structure containing database connection and dump configuration information
- `*numTablesPtr`: Output parameter that receives the count of tables found in the database
## Dependencies
- Functions called/Symbols referenced:
  - [getExtensions](getExtensions.md) (reads extension information first)
  - [getExtensionMembership](getExtensionMembership.md) (identifies extension members)
  - [getNamespaces](getNamespaces.md) (reads schema information)
  - [getTables](getTables.md) (reads table metadata)
  - [getTypes](getTypes.md) (reads user-defined types)
  - [getFuncs](getFuncs.md) (reads user-defined functions)
  - [flagInhTables](../f/flagInhTables.md) (processes inheritance relationships)
  - [flagInhAttrs](../f/flagInhAttrs.md) (flags inherited columns)
  - [flagInhIndexes](../f/flagInhIndexes.md) (flags inherited indexes)
  - [getIndexes](getIndexes.md), getConstraints, getTriggers (reads table-related objects)
  - [getPublications](getPublications.md), getSubscriptions (reads logical replication objects)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_dump/pg_dump.c:956)

## Notes and Other Information
The function follows a strict ordering to ensure proper dependency resolution:
1. Extensions first (affects dumping decisions for other objects)
2. Namespaces second (tables need to link to their schemas)
3. Tables early (minimizes lock acquisition window)
4. Functions and types (may have interdependencies)
5. Inheritance processing after basic objects are loaded
6. Indexes and constraints after table structure is complete

The function returns a TableInfo array which serves as the primary data structure for subsequent dump operations, containing not only table information but also serving as an anchor point for related objects.

## Simplified Source

```c
TableInfo *
getSchemaData(Archive *fout, int *numTablesPtr)
{
    TableInfo *tblinfo;
    ExtensionInfo *extinfo;
    InhInfo *inhinfo;
    int numTables, numExtensions, numInherits;
    // (other count variables declared)

    // Phase 1: Extensions (affects dumping decisions for other objects)
    pg_log_info("reading extensions");
    extinfo = getExtensions(fout, &numExtensions);
    getExtensionMembership(fout, extinfo, numExtensions);

    // Phase 2: Core schema objects
    pg_log_info("reading schemas");
    getNamespaces(fout, &numNamespaces);

    pg_log_info("reading user-defined tables");
    tblinfo = getTables(fout, &numTables);
    getOwnedSeqs(fout, tblinfo, numTables);

    // Phase 3: Functions and types (order matters for dependencies)
    pg_log_info("reading user-defined functions");
    getFuncs(fout, &numFuncs);

    pg_log_info("reading user-defined types");
    getTypes(fout, &numTypes);

    // Phase 4: Other database objects
    // (collect operators, aggregates, access methods, etc.)

    // Phase 5: Relationships and detailed info
    pg_log_info("reading table inheritance information");
    inhinfo = getInherits(fout, &numInherits);

    pg_log_info("finding inheritance relationships");
    flagInhTables(fout, tblinfo, numTables, inhinfo, numInherits);

    pg_log_info("reading column info for interesting tables");
    getTableAttrs(fout, tblinfo, numTables);

    // Phase 6: Table-related objects (indexes, constraints, triggers)
    pg_log_info("reading indexes");
    getIndexes(fout, tblinfo, numTables);

    pg_log_info("reading constraints");
    getConstraints(fout, tblinfo, numTables);

    // Cleanup and return
    free(inhinfo);
    *numTablesPtr = numTables;
    return tblinfo;
}
```