# listTSParsersVerbose

## Location
src/bin/psql/describe.c: 5199 - 5273

## Overview
Provides detailed information about text search parsers matching a given pattern by querying PostgreSQL's text search parser catalog and displaying verbose descriptions for each parser.

## Definition
static bool listTSParsersVerbose(const char *pattern)

## Detailed Description
This function implements the verbose listing functionality for PostgreSQL text search parsers in psql. It queries the pg_ts_parser catalog to retrieve parser information including OID, namespace, and parser name. For each matching parser, it calls describeOneTSParser to display detailed information about the parser's functions and token types. The function supports pattern matching for selective parser listing and provides appropriate error messages when no parsers are found.

## Parameters / Member Variables
- : Pattern string for filtering parsers by name; if NULL, lists all visible parsers

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [describeOneTSParser](../d/describeOneTSParser.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [listTSParsers](listTSParsers.md)

## Notes and Other Information
- Returns false on error or when no parsers match the pattern
- Handles cancellation through the cancel_pressed global variable
- Uses PostgreSQL's visibility rules via pg_ts_parser_is_visible function
- Provides user-friendly error messages when no parsers are found
- Part of psql's \dFp+ command implementation for verbose parser listing