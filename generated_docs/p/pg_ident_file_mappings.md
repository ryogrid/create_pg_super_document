# pg_ident_file_mappings

## Location
src/backend/utils/adt/hbafuncs.c: 574 - 591

## Overview
A SQL-accessible set-returning function (SRF) that returns all entries from the pg_ident.conf file as a system view.

## Definition


## Detailed Description
This function implements the pg_ident_file_mappings system view, which provides SQL access to PostgreSQL's identity mapping configuration stored in pg_ident.conf. The function is designed as a set-returning function that materializes all identity mapping entries into a tuplestore for safe and efficient access.

The function performs the following operations:
1. Initializes a materialized set-returning function context using InitMaterializedSRF()
2. Delegates the actual file parsing and data population to fill_ident_view()
3. Returns the populated tuplestore to the SQL engine

The materialized approach ensures that the view remains consistent even if the pg_ident.conf file is modified while a query cursor is open, and provides better performance compared to streaming approaches that would need to maintain position state in the parsed file.

## Parameters / Member Variables
- Uses standard PostgreSQL function calling convention (PG_FUNCTION_ARGS)
- No explicit parameters - operates on the current pg_ident.conf file
- Returns Datum (standard PostgreSQL function return type)

## Dependencies  
- Functions called/Symbols referenced:
  - InitMaterializedSRF
  - fill_ident_view
  - PG_RETURN_NULL
- Called from (representative examples):
  - SQL queries accessing pg_ident_file_mappings view
  - PostgreSQL system catalog access

## Notes and Other Information
- This function is exposed to SQL as a system view/function, allowing administrators to query identity mapping configurations
- Uses materialized SRF mode for safety against concurrent file modifications and improved performance
- The function itself performs minimal work, delegating the complex parsing logic to fill_ident_view()
- Part of PostgreSQL's system information functions that provide introspection into server configuration
- Returns NULL as per standard SRF convention - actual data is returned via the tuplestore
- Located in src/backend/utils/adt/hbafuncs.c:574-591
- Typically used in system administration queries to inspect current identity mapping rules