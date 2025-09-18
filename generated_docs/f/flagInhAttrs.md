# flagInhAttrs

## Location
[src/bin/pg_dump/common.c:501-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L501-L645)

## Overview
Identifies inherited column attributes and optimizes their representation in dump output to avoid redundancy and ensure proper restoration.

## Definition


## Detailed Description
The flagInhAttrs function analyzes inheritance relationships between tables to optimize how column attributes are represented in the pg_dump output. It performs three critical optimizations: (1) flags columns that inherit NOT NULL constraints from parents to avoid redundant specifications, (2) creates explicit DEFAULT NULL clauses for child columns that need to override inherited non-null defaults, and (3) suppresses generation expressions in child tables when they match all parent generation expressions, improving compatibility with pre-v16 PostgreSQL servers.

The function carefully handles the complex inheritance semantics of PostgreSQL, where child tables can inherit constraints, defaults, and generation expressions from their parents. By identifying which attributes are truly inherited versus locally defined, it ensures that the dump output is both minimal and semantically correct during restoration.

## Parameters / Member Variables
- : Archive structure containing database connection and dump configuration options
- : Array of TableInfo structures representing all tables in the database
- : Number of tables in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [strInArray](../s/strInArray.md) (searches for matching column names in parent tables)
  - [AssignDumpId](../A/AssignDumpId.md) (assigns dump IDs to manufactured AttrDefInfo objects)
  - [shouldPrintColumn](../s/shouldPrintColumn.md) (determines if column will be explicitly dumped)
  - [addObjectDependency](../a/addObjectDependency.md) (establishes dependencies for separate default clauses)
  - pg_malloc_object (memory allocation for AttrDefInfo)
- Called from (representative examples):
  - [getSchemaData](../g/getSchemaData.md) (src/bin/pg_dump/common.c:233)

## Notes and Other Information
The function processes tables in OID order but cannot assume parents are visited before children, requiring careful state management to avoid altering properties that affect other iterations. It creates synthetic AttrDefInfo objects for DEFAULT NULL clauses when children need to explicitly override inherited non-null defaults.

Special handling exists for generation expressions: they are suppressed in child tables only when all parents have identical expressions, except for partitions and binary upgrade mode where explicit specification is required. The function only processes regular tables, excluding sequences, views, and materialized views which don't participate in inheritance.