# getRules

## Location
src/bin/pg_dump/pg_dump.c: 8124 - 8224

## Overview
Retrieves comprehensive information about all PostgreSQL rules in the system from the pg_rewrite catalog, preparing them for database dump operations.

## Definition


## Detailed Description
This function queries the pg_rewrite system catalog to collect information about all rules defined in the database. Rules in PostgreSQL are used to implement views, materialized views, and custom query rewriting logic. The function creates RuleInfo structures for each rule, establishing proper dependency relationships between rules and their associated tables.

The function handles special cases for view and materialized view rules, particularly the ON SELECT rules that define view behavior. For these rules, it establishes dependencies to ensure proper dump ordering - view-defining rules are processed before their tables to handle dependencies correctly, while other rules are processed after their tables.

Key features include:
- Comprehensive rule information extraction from pg_rewrite catalog
- Proper dependency management between rules and tables
- Special handling for view and materialized view SELECT rules
- Memory allocation and structure initialization for all discovered rules
- Integration with the dump system's dependency tracking

## Parameters / Member Variables
- : Archive pointer containing database connection and dump context information
- : Output parameter that receives the total number of rules found

## Dependencies
- Functions called/Symbols referenced:
  - [RuleInfo](../R/RuleInfo.md) (struct type)
  - createPQExpBuffer, appendPQExpBufferStr (query building functions)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (query execution)
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue (libpq result processing functions)
  - pg_malloc (memory allocation)
  - atooid (string to OID conversion)
  - [AssignDumpId](../A/AssignDumpId.md) (dump ID assignment)
  - [findTableByOid](../f/findTableByOid.md) (table lookup function)
  - [addObjectDependency](../a/addObjectDependency.md) (dependency management)
  - destroyPQExpBuffer (cleanup)
  - DO_RULE (object type enum)
  - RELKIND_VIEW, RELKIND_MATVIEW (relation kind enums)
  - PGRES_TUPLES_OK (libpq result status)

- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) (primary caller during schema data collection phase)

## Notes and Other Information
- Returns a dynamically allocated array of RuleInfo structures
- The caller is responsible for managing the returned memory
- Includes sophisticated dependency logic for view rules: ON SELECT INSTEAD rules for views/materialized views are made dependencies of their tables, while other rules depend on their tables
- Handles rule enablement status (ev_enabled) for conditional rule execution
- Performs sanity checking to ensure referenced tables exist
- Rules for views may be merged into CREATE VIEW statements rather than dumped separately (separate = false)
- The function processes rules in OID order to ensure consistent dump output
- Fatal errors occur if referenced tables cannot be found, indicating catalog corruption