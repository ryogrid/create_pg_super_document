# getOpfamilies

## Location
src/bin/pg_dump/pg_dump.c: 6389 - 6459

## Overview
The getOpfamilies function retrieves all operator families from the PostgreSQL system catalogs and returns them in an OpfamilyInfo structure array for use by pg_dump.

## Definition


## Detailed Description
This function is part of pg_dump's catalog reading functionality. It executes a SQL query against the pg_opfamily system catalog to retrieve all operator families in the database, including both user-defined and built-in operator families. The function creates OpfamilyInfo structures for each operator family, populating them with essential metadata such as OID, name, namespace, access method, and owner information. System-defined operator families are filtered out later during the dump-out phase rather than during this collection phase.

The function allocates memory for an array of OpfamilyInfo structures and initializes each structure with data from the query result. It also assigns dump IDs to each operator family and determines whether each should be dumped based on the current dump configuration.

## Parameters / Member Variables
- : Archive structure containing connection and dump configuration information
- : Pointer to integer that will be set to the number of operator families found

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - appendPQExpBufferStr
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
  - selectDumpableObject
  - PQclear
  - destroyPQExpBuffer
- Called from (representative examples):
  - getSchemaData

## Notes and Other Information
- The function queries the pg_opfamily system catalog to retrieve operator family metadata
- Memory allocation is performed using pg_malloc for the OpfamilyInfo array
- The function uses the DO_OPFAMILY object type identifier for dump objects
- System-defined operator families are included in the collection but filtered during dump output
- Each operator family is assigned a unique dump ID for dependency tracking
- The function properly handles PostgreSQL result set processing and memory cleanup