# _tmplInfo

## Location
[src/bin/pg_dump/pg_dump.h:551-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L551-L555)

## Overview
The  structure represents text search template information in the PostgreSQL dump utility, storing metadata about dictionary templates for database export operations.

## Definition


## Detailed Description
This structure is part of the pg_dump utility's internal representation of database objects. It stores information about text search dictionary templates, which define the behavior and functionality that text search dictionaries inherit. Dictionary templates provide the foundation for creating dictionaries used in PostgreSQL's full-text search system.

## Parameters / Member Variables
- : Base  structure containing common dump metadata (name, namespace, dependencies, etc.)
- : OID of the template's initialization function, which sets up the dictionary instance when created
- : OID of the template's lexicalization function, which processes tokens during text search operations

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found in the analyzed code

## Notes and Other Information
- This structure is defined in  at lines 551-555
- Part of PostgreSQL's text search infrastructure support in pg_dump
- Dictionary templates serve as blueprints for creating text search dictionaries
- The tmplinit function is called when a dictionary based on this template is created
- The tmpllexize function performs the actual text processing work during search operations
- The structure inherits from DumpableObject, making it part of pg_dump's standard object dumping framework