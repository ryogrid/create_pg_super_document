# _dictInfo

## Location
[src/bin/pg_dump/pg_dump.h:543-548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L543-L548)

## Overview
The  structure represents text search dictionary information in the PostgreSQL dump utility, storing metadata about text search dictionaries for database export operations.

## Definition


## Detailed Description
This structure is part of the pg_dump utility's internal representation of database objects. It stores information about text search dictionaries, which are key components of PostgreSQL's full-text search system. Text search dictionaries are used to process tokens during text search operations, performing tasks like stemming, synonym replacement, and stop word filtering.

## Parameters / Member Variables
- : Base  structure containing common dump metadata (name, namespace, dependencies, etc.)
- : Name of the role (user) that owns this dictionary
- : OID of the dictionary template that this dictionary is based on
- : Initialization options string passed to the dictionary template during creation

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found in the analyzed code

## Notes and Other Information
- This structure is defined in  at lines 543-548
- Part of PostgreSQL's text search infrastructure support in pg_dump
- Dictionary templates define the behavior while dictinitoption provides instance-specific configuration
- The structure inherits from DumpableObject, making it part of pg_dump's standard object dumping framework
- Text search dictionaries work with parsers and configurations to provide full-text search capabilities