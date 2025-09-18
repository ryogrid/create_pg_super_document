# _inhInfo

## Location
[src/bin/pg_dump/pg_dump.h:527-530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L527-L530)

## Overview
The  structure represents temporary inheritance information in PostgreSQL's pg_dump utility, used to track table inheritance relationships during dump operations.

## Definition


## Detailed Description
The  structure is a lightweight temporary data structure used internally by pg_dump to manage table inheritance relationships. Unlike other pg_dump structures, it does not inherit from  because it represents transient state rather than a database object that needs to be dumped. This structure is used to track which tables are child tables in inheritance hierarchies during the dump process.

## Parameters / Member Variables
- : OID of a child table that inherits from one or more parent tables

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic Oid type)
- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- This structure is explicitly noted as not being a , distinguishing it from other pg_dump data structures
- It serves as temporary state information rather than representing a persistent database object
- The structure is used internally to track inheritance relationships while processing table dependencies
- Its minimal design (single OID field) reflects its specific purpose of temporarily holding child table identifiers
- This structure helps pg_dump properly order table creation to respect inheritance hierarchies during database restoration
- The inheritance relationship details (parent tables, inheritance options, etc.) are likely stored elsewhere in more comprehensive structures