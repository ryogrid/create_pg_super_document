# _cfgInfo

## Location
[src/bin/pg_dump/pg_dump.h:558-562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L558-L562)

## Overview
The  structure represents text search configuration information in the PostgreSQL dump utility, storing metadata about text search configurations for database export operations.

## Definition

```c
typedef struct _cfgInfo
{
	DumpableObject dobj;
	const char *rolname;
	Oid			cfgparser;
} TSConfigInfo;
```
## Detailed Description
This structure is part of the pg_dump utility's internal representation of database objects. It stores information about text search configurations, which are high-level objects that coordinate the use of parsers and dictionaries in PostgreSQL's full-text search system. Text search configurations define how documents are processed for indexing and searching.

## Parameters / Member Variables
- : Base  structure containing common dump metadata (name, namespace, dependencies, etc.)
- : Name of the role (user) that owns this text search configuration
- : OID of the text search parser associated with this configuration

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found in the analyzed code

## Notes and Other Information
- This structure is defined in  at lines 558-562
- Part of PostgreSQL's text search infrastructure support in pg_dump
- Text search configurations tie together parsers and dictionaries to provide complete full-text search functionality
- The configuration specifies which parser to use for breaking text into tokens
- Additional mapping information (not stored in this structure) defines which dictionaries to apply to different token types
- The structure inherits from DumpableObject, making it part of pg_dump's standard object dumping framework