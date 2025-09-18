# getProcLangs

## Location
src/bin/pg_dump/pg_dump.c: 8508 - 8597

## Overview
Retrieves basic information about every procedural language in the PostgreSQL system for use by pg_dump during database backup operations.

## Definition


## Detailed Description
The  function queries the  system catalog to retrieve information about all procedural languages that have the  flag set to true (indicating they are procedural languages rather than built-in languages). This function is part of the pg_dump utility's schema dumping process and must be called after  because it assumes that  functionality is available.

The function constructs a SQL query to fetch language metadata including permissions (ACLs), ownership, trusted status, and associated function OIDs. For each language found, it creates a  structure containing all relevant information needed for dumping the language definition.

## Parameters / Member Variables
- : Archive pointer for the pg_dump operation, used for executing SQL queries
- : Output parameter that receives the number of procedural languages found

## Dependencies
- Functions called/Symbols referenced:
  - ExecuteSqlQuery
  - ProcLangInfo
  - pg_malloc
  - atooid
  - AssignDumpId
  - getRoleName
  - selectDumpableProcLang
  - PQgetisnull
- Called from (representative examples):
  - getSchemaData

## Notes and Other Information
- Must be executed after getFuncs() due to dependency on findFuncByOid() functionality
- Queries only procedural languages (lanispl = true), excluding built-in languages
- Results are ordered by OID for consistent output
- Each language's dumpability is determined by selectDumpableProcLang()
- ACL information is preserved for languages that have explicit permissions set
- Memory allocation is performed for the entire result set at once using pg_malloc