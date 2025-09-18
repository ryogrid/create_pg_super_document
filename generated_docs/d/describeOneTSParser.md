# describeOneTSParser

## Location
src/bin/psql/describe.c: 5274 - 5393

## Overview
Displays detailed information about a specific text search parser, including its constituent functions and supported token types.

## Definition
static bool describeOneTSParser(const char *oid, const char *nspname, const char *prsname)

## Detailed Description
This function provides comprehensive details about a single text search parser by executing two separate queries. The first query retrieves information about the parser's five core functions (start parse, get next token, end parse, get headline, and get token types) along with their descriptions. The second query calls the ts_token_type function to display all token types supported by the parser. Results are formatted and displayed using psql's table formatting system with appropriate titles and column translations.

## Parameters / Member Variables
- `oid`: Object identifier of the text search parser in pg_ts_parser catalog
- `nspname`: Namespace name of the parser (may be NULL for parsers in current search path)
- `prsname`: Name of the text search parser

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)  
  - [PSQLexec](../P/PSQLexec.md)
  - termPQExpBuffer
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
  - lengthof
- Called from (representative examples):
  - [listTSParsersVerbose](../l/listTSParsersVerbose.md)

## Notes and Other Information
- Returns false on error, true on success
- Uses UNION ALL queries to display parser function information in a structured format
- Implements internationalization through gettext_noop for column headers
- Displays two separate result tables: parser functions and token types
- Part of psql's \dFp+ command implementation for detailed parser inspection
- Handles both schema-qualified and unqualified parser names in display titles