# listTSDictionaries

## Location
[src/bin/psql/describe.c:5394-5458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5394-L5458)

## Overview
Lists PostgreSQL text search dictionaries with optional verbose details including template information and initialization options.

## Definition
bool listTSDictionaries(const char *pattern, bool verbose)

## Detailed Description
This function implements the \dFd psql command for listing text search dictionaries from the pg_ts_dict catalog. It queries dictionary information including schema, name, and description. When verbose mode is enabled, it additionally displays the associated template name (with namespace) and dictionary initialization options. The function supports pattern matching for selective dictionary listing and uses PostgreSQL's visibility rules to show only accessible dictionaries.

## Parameters / Member Variables
- `pattern`: Pattern string for filtering dictionaries by name; if NULL, lists all visible dictionaries
- `verbose`: Boolean flag to enable verbose output showing template and initialization options

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command processor)

## Notes and Other Information
- Returns false on error, true on success
- Uses pg_ts_dict_is_visible function to respect PostgreSQL's visibility rules
- In verbose mode, joins with pg_ts_template and pg_namespace to show complete template information
- Handles null namespace gracefully by displaying '(null)' for system templates
- Part of psql's text search object inspection functionality
- Results are ordered by schema name, then dictionary name
- Implements internationalization through gettext_noop for column headers