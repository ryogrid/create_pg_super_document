# listTSConfigs

## Location
src/bin/psql/describe.c: 5524 - 5572

## Overview
Lists PostgreSQL text search configurations with basic information, delegating to listTSConfigsVerbose for detailed output when verbose mode is requested.

## Definition
bool listTSConfigs(const char *pattern, bool verbose)

## Detailed Description
This function implements the \dF psql command for listing text search configurations from the pg_ts_config catalog. In non-verbose mode, it queries basic configuration information including schema, name, and description. When verbose mode is enabled, it delegates to listTSConfigsVerbose function for comprehensive configuration details including parser and dictionary mappings. The function supports pattern matching for selective configuration listing and uses PostgreSQL's visibility rules to show only accessible configurations.

## Parameters / Member Variables
- `pattern`: Pattern string for filtering configurations by name; if NULL, lists all visible configurations
- `verbose`: Boolean flag that determines output detail level; when true, delegates to listTSConfigsVerbose

## Dependencies
- Functions called/Symbols referenced:
  - listTSConfigsVerbose (when verbose=true)
  - initPQExpBuffer
  - printfPQExpBuffer
  - validateSQLNamePattern
  - termPQExpBuffer
  - PSQLexec
  - printQuery
  - PQclear
  - gettext_noop
- Called from (representative examples):
  - exec_command_d (psql command processor)

## Notes and Other Information
- Returns false on error, true on success
- Uses pg_ts_config_is_visible function to respect PostgreSQL's visibility rules
- Serves as a dispatcher function, handling simple listings internally and complex ones via delegation
- Text search configurations define how documents are parsed and indexed for full-text search
- Part of psql's text search object inspection functionality
- Results are ordered by schema name, then configuration name
- Implements internationalization through gettext_noop for column headers
- Provides a clean separation between simple and verbose configuration listing