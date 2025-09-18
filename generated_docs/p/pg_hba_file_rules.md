# pg_hba_file_rules

## Location
src/backend/utils/adt/hbafuncs.c: 430 - 449

## Overview
SQL-accessible set-returning function that returns all entries from the pg_hba.conf file as a result set for the pg_hba_file_rules system view.

## Definition


## Detailed Description
The  function serves as the SQL interface for the pg_hba_file_rules system view, which exposes PostgreSQL's host-based authentication configuration to SQL queries. It implements a set-returning function (SRF) that reads and parses the entire pg_hba.conf file, returning each configuration rule as a separate row. The function uses PostgreSQL's materialized SRF infrastructure to ensure thread-safety against concurrent HBA file changes and provide efficient random access to results. All the actual processing is delegated to the fill_hba_view function, while this function handles the SQL interface aspects.

## Parameters / Member Variables
- Takes standard PostgreSQL function arguments via PG_FUNCTION_ARGS macro
- No explicit parameters - uses PostgreSQL's function call context

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [fill_hba_view](../f/fill_hba_view.md)
  - PG_RETURN_NULL
- Types referenced:
  - ReturnSetInfo
  - Datum (return type)
- Called from:
  - SQL queries accessing pg_hba_file_rules system view

## Notes and Other Information
- Public function exposed as a PostgreSQL system view
- Uses materialized mode for thread-safety against HBA file changes during cursor operations
- Provides more efficient access than re-parsing the file for each row access
- Part of PostgreSQL's system catalog infrastructure for configuration introspection
- Returns Datum type as required by PostgreSQL's function interface
- The actual logic is implemented in fill_hba_view for better code organization
- Registered in the system catalog to enable SQL access to HBA configuration