# getIndexes

## Location
src/bin/pg_dump/pg_dump.c: 7433 - 7742

## Overview
Retrieves comprehensive information about all indexes on dumpable tables and creates corresponding DumpableObject entries for use during pg_dump operations.

## Definition


## Detailed Description
The getIndexes function performs a sophisticated query against PostgreSQL system catalogs to gather complete index information for all tables marked for dumping. It constructs a single optimized SQL query that retrieves index definitions, statistics, constraint relationships, and metadata from multiple system tables including pg_index, pg_class, pg_constraint, and pg_inherits. The function handles version-specific features like replica identity indexes (9.4+), partitioned indexes (11.0+), and NULLS NOT DISTINCT support (15.0+). For each index found, it creates IndxInfo structures and populates them with detailed metadata. Additionally, when indexes are associated with constraints (primary key, unique, or exclusion), it creates corresponding ConstraintInfo entries, establishing proper dependency relationships for correct dump ordering.

## Parameters / Member Variables
- : Archive pointer containing dump configuration and database connection information
- : Array of TableInfo structures representing tables to be dumped
- : Number of entries in the tblinfo array

## Dependencies
- Functions called/Symbols referenced:
  - TableInfo (structure type)
  - IndxInfo (structure type)
  - appendPQExpBufferChar
  - ExecuteSqlQuery
  - PGRES_TUPLES_OK (constant)
  - pg_malloc
  - atooid
  - DO_INDEX (enum value)
  - AssignDumpId
  - parseOidArray
  - SimplePtrList (structure type)
  - ConstraintInfo (structure type)
  - DO_CONSTRAINT (enum value)
- Called from (representative examples):
  - getSchemaData
  - SubRelInfo (referenced in header)

## Notes and Other Information
- Only processes tables that have the hasindex flag set and are marked as interesting
- Builds an OID array of target tables to optimize the single SQL query approach
- Handles PostgreSQL version differences with conditional SQL generation
- Creates constraint entries for primary key, unique, and exclusion constraint indexes
- Supports partitioned index inheritance relationships (PostgreSQL 11+)
- Retrieves index statistics columns and values for performance analysis
- The function assumes tblinfo array is sorted by OID for efficient table lookup
- Index data is stored in TableInfo structures rather than returned directly
- Memory management includes proper allocation for IndxInfo arrays and string fields