# listTSTemplates

## Location
[src/bin/psql/describe.c:5459-5523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5459-L5523)

## Overview
Lists PostgreSQL text search templates with optional verbose details including initialization and lexize function information.

## Definition
bool listTSTemplates(const char *pattern, bool verbose)

## Detailed Description
This function implements the \dFt psql command for listing text search templates from the pg_ts_template catalog. It queries template information including schema, name, and description. When verbose mode is enabled, it additionally displays the template's initialization function (tmplinit) and lexize function (tmpllexize) that are used for dictionary creation and text processing. The function supports pattern matching for selective template listing and uses PostgreSQL's visibility rules to show only accessible templates.

## Parameters / Member Variables
- `pattern`: Pattern string for filtering templates by name; if NULL, lists all visible templates  
- `verbose`: Boolean flag to enable verbose output showing initialization and lexize functions

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command processor)

## Notes and Other Information
- Returns false on error, true on success
- Uses pg_ts_template_is_visible function to respect PostgreSQL's visibility rules
- In verbose mode, displays function names as regproc types for better readability
- Templates define the behavior patterns used by text search dictionaries
- Part of psql's text search object inspection functionality
- Results are ordered by schema name, then template name
- Implements internationalization through gettext_noop for column headers
- Supports both simple listing and detailed function inspection modes