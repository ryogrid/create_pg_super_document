# SPI_prepare

## Location
src/backend/executor/spi.c: 860 - 865

## Overview
SPI_prepare parses and plans a SQL query with parameter placeholders, returning a reusable plan that can be executed multiple times with different parameter values.

## Definition


## Detailed Description
SPI_prepare is a convenience wrapper around SPI_prepare_cursor that creates a prepared statement plan from a SQL query string. It parses the query, validates the parameter types, and creates an execution plan that can be reused multiple times with different parameter values. The function delegates to SPI_prepare_cursor with cursor_options set to 0 (no special cursor options).

This function is fundamental to the SPI interface and is widely used throughout PostgreSQL for creating reusable query plans, particularly in triggers, stored procedures, and procedural languages.

## Parameters / Member Variables
- `src`: const char * - The SQL query string to prepare (may contain parameter placeholders like , , etc.)
- `nargs`: int - Number of parameters expected by the query
- `argtypes`: Oid * - Array of PostgreSQL type OIDs for the parameters

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_prepare_cursor](SPI_prepare_cursor.md)
  - [SPIPlanPtr](SPIPlanPtr.md)
- Called from (representative examples):
  - [RI_Initial_Check](../R/RI_Initial_Check.md)
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md)
  - [ri_PlanCheck](../r/ri_PlanCheck.md)
  - [pg_get_ruledef_worker](../p/pg_get_ruledef_worker.md)
  - [pg_get_viewdef_worker](../p/pg_get_viewdef_worker.md)
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md)
  - [ts_stat_sql](../t/ts_stat_sql.md)
  - [query_to_xmlschema](../q/query_to_xmlschema.md)
  - [query_to_xml_and_xmlschema](../q/query_to_xml_and_xmlschema.md)
  - [plperl_spi_prepare](../p/plperl_spi_prepare.md)
  - [PLy_spi_prepare](../P/PLy_spi_prepare.md)
  - pltcl_SPI_prepare

## Notes and Other Information
- Returns SPIPlanPtr on success, NULL on failure
- The returned plan must be freed with SPI_freeplan when no longer needed
- Parameter placeholders in the SQL should be written as , , etc.
- The plan can be executed multiple times using SPI_execute_plan or related functions
- Widely used across PostgreSQL's procedural languages (PL/Perl, PL/Python, PL/Tcl)
- Used extensively in referential integrity triggers and utility functions
- This is the standard way to create prepared statements in SPI
- The plan is cached and can provide performance benefits for repeated execution