# set_restrict_relation_kind

## Location
src/bin/pg_dump/pg_dump.c: 4777 - 4797

## Overview
Sets the restrict_nonsystem_relation_kind configuration parameter in PostgreSQL by executing a dynamic SQL query to conditionally apply this setting only when it's available in the server version.

## Definition


## Detailed Description
This function sets the  configuration parameter using PostgreSQL's  function. The parameter was introduced in minor version releases, so the function uses a conditional query that only applies the setting if the parameter exists in . This ensures backward compatibility across different PostgreSQL versions where this parameter may not be available.

The function constructs and executes a SQL query that searches for the parameter name in  and only calls  if found, preventing errors on older PostgreSQL versions that don't have this configuration parameter.

## Parameters / Member Variables
- : Archive handle containing database connection information for executing the SQL query
- : String value to set for the restrict_nonsystem_relation_kind parameter

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - fmtQualifiedDumpable
  - [setup_connection](setup_connection.md)
  - [dumpTableData_copy](../d/dumpTableData_copy.md)
  - [dumpTableData_insert](../d/dumpTableData_insert.md)

## Notes and Other Information
- This function is specifically designed for minor version compatibility where new configuration parameters are introduced
- The query uses a conditional SELECT to avoid errors when the parameter doesn't exist
- Used internally by pg_dump to control which system relations are included in dumps