# _prsInfo

## Location
[src/bin/pg_dump/pg_dump.h:533-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L533-L540)

## Overview
The  structure represents text search parser information in the PostgreSQL dump utility, storing metadata about text search parsers for database export operations.

## Definition


## Detailed Description
This structure is part of the pg_dump utility's internal representation of database objects. It stores information about text search parsers, which are components of PostgreSQL's full-text search system. The structure contains OIDs (Object Identifiers) for various function components that make up a text search parser.

## Parameters / Member Variables
- : Base  structure containing common dump metadata (name, namespace, dependencies, etc.)
- : OID of the parser's start function, which initializes parsing
- : OID of the parser's token function, which extracts the next token from input
- : OID of the parser's end function, which performs cleanup after parsing
- : OID of the parser's headline function, which generates search result headlines
- : OID of the parser's lexical type function, which returns information about token types

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found in the analyzed code

## Notes and Other Information
- This structure is defined in  at lines 533-540
- Part of PostgreSQL's text search infrastructure support in pg_dump
- Each OID field corresponds to a specific function that implements part of the text search parser's functionality
- The structure inherits from DumpableObject, making it part of pg_dump's standard object dumping framework