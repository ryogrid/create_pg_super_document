# listTSConfigsVerbose

## Location
[src/bin/psql/describe.c:5573-5656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5573-L5656)

## Overview
Lists all text search configurations in the PostgreSQL database with detailed information, including their associated parsers and namespaces.

## Definition
static bool listTSConfigsVerbose(const char *pattern)

## Detailed Description
This function queries the PostgreSQL system catalogs to retrieve comprehensive information about text search configurations. It performs a JOIN operation across multiple catalog tables (pg_ts_config, pg_namespace, pg_ts_parser) to gather configuration names, their namespaces, associated parsers, and parser namespaces. For each configuration found, it calls describeOneTSConfig to display detailed token-to-dictionary mappings. The function supports pattern matching to filter results and provides verbose output showing the internal structure of text search configurations.

## Parameters / Member Variables
- : Optional SQL pattern to filter text search configuration names (can be NULL to show all configurations)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) 
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [describeOneTSConfig](../d/describeOneTSConfig.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [listTSConfigs](listTSConfigs.md)

## Notes and Other Information
- Returns false if no configurations match the pattern or if an error occurs
- Displays error messages when no configurations are found (unless pset.quiet is set)
- Supports cancellation via cancel_pressed global variable
- Orders results by namespace and configuration name for consistent output
- This is a static function used internally by psql's describe functionality for text search configurations