# dumpUserMappings

## Location
[src/bin/pg_dump/pg_dump.c:15079-15172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15079-L15172)

## Overview
Dumps all user mappings associated with a specific foreign server, generating CREATE USER MAPPING statements for database restoration.

## Definition


## Detailed Description
The  function is responsible for extracting and dumping all user mappings that are associated with a particular foreign server. It queries the  view to retrieve user mapping information and generates the corresponding SQL DDL statements for database restoration.

Key features include:
1. **Non-superuser friendly** - Uses the publicly accessible  view instead of direct system catalog access
2. **Options handling** - Properly formats user mapping options, with special handling for cases where users lack privileges to see sensitive options
3. **Comprehensive SQL generation** - Creates both CREATE and DROP statements for each user mapping
4. **Proper formatting** - Formats options with appropriate indentation and escaping

The function is typically called after a foreign server has been archived to ensure that all related user mappings are also preserved in the dump.

## Parameters / Member Variables
- : Archive structure for writing the generated SQL statements
- : Name of the foreign server whose user mappings should be dumped
- : Schema namespace context for the user mappings
- : Owner of the parent foreign server object
- : Catalog identifier of the foreign server (used in the SQL query)
- : Dump identifier for dependency tracking

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [fmtId](../f/fmtId.md)
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [dumpForeignServer](dumpForeignServer.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function uses  view for security reasons - non-superusers can execute dumps without failing
- When users lack privileges for a server,  appears as null, resulting in user mappings without OPTIONS clauses
- User mappings are sorted by username for consistent output
- Each user mapping gets its own archive entry with proper tagging and dependency relationships
- The function handles proper memory management with multiple PQExpBuffer objects for different purposes (query, create statement, drop statement, tag)