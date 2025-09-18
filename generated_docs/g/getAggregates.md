# getAggregates

## Location
src/bin/pg_dump/pg_dump.c: 6460 - 6606

## Overview
The getAggregates function retrieves all user-defined aggregate functions from the PostgreSQL system catalogs and returns them in an AggInfo structure array for use by pg_dump.

## Definition


## Detailed Description
This function is part of pg_dump's catalog reading functionality that specifically handles aggregate functions. It constructs and executes a complex SQL query against the pg_proc system catalog to retrieve user-defined aggregates, filtering out system-defined aggregates in pg_catalog unless they have custom privileges. The function handles different PostgreSQL versions, using different aggregate identification methods (proisagg for older versions, prokind = 'a' for PostgreSQL 11+).

The function creates AggInfo structures for each aggregate, populating them with comprehensive metadata including OID, name, namespace, argument types, owner, and access control information. It also handles argument type parsing for aggregates that take parameters and manages ACL (Access Control List) information for privilege management during dump/restore operations.

## Parameters / Member Variables
- : Archive structure containing connection information and dump configuration options
- : Pointer to integer that will be set to the number of aggregates found

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - appendPQExpBuffer
  - appendPQExpBufferStr
  - appendPQExpBufferChar
  - ExecuteSqlQuery
  - PQntuples
  - pg_malloc
  - PQfnumber
  - PQgetvalue
  - atooid
  - AssignDumpId
  - pg_strdup
  - findNamespace
  - getRoleName
  - atoi
  - parseOidArray
  - selectDumpableObject
  - PQgetisnull
  - PQclear
  - destroyPQExpBuffer
- Called from (representative examples):
  - getSchemaData

## Notes and Other Information
- The function uses version-specific SQL queries to handle changes in PostgreSQL's aggregate identification (proisagg vs prokind)
- System aggregates in pg_catalog are filtered out unless they have custom privileges or are part of extensions during binary upgrades
- ACL information is preserved for aggregates that have custom privileges
- Argument types are parsed from the proargtypes array and stored as separate Oid arrays
- The function handles both parameterized and non-parameterized aggregates
- Uses DO_AGG object type identifier for dump object classification
- Supports binary upgrade mode with special handling for extension-dependent aggregates