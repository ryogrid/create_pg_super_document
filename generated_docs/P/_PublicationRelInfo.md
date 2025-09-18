# _PublicationRelInfo

## Location
[src/bin/pg_dump/pg_dump.h:648-654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L648-L654)

## Overview
The  struct represents publication table mapping, used by pg_dump to store information about tables that are included in logical replication publications.

## Definition


## Detailed Description
This structure is part of PostgreSQL's pg_dump utility and is used to maintain the relationship between publications and the tables they contain. It stores metadata about how a specific table is included in a publication, including any row filters and column lists that may be applied. This information is essential for accurately recreating publication configurations during database dumps and restores.

## Parameters / Member Variables
- : Base DumpableObject structure containing common metadata for dump objects
- : Pointer to the PublicationInfo structure representing the publication this table belongs to
- : Pointer to the TableInfo structure representing the table that is part of the publication
- : String containing the WHERE clause (row filter) applied to this table in the publication, or NULL if no filter
- : String containing the column list for this table in the publication, or NULL if all columns are included

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - PublicationInfo
  - [TableInfo](../T/TableInfo.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This structure is defined in src/bin/pg_dump/pg_dump.h at lines 648-654
- It's used specifically by the pg_dump utility for logical replication publication handling
- The struct helps maintain the many-to-many relationship between publications and tables
- Row filters (pubrelqual) and column lists (pubrattrs) are stored as strings and parsed when needed during dump operations