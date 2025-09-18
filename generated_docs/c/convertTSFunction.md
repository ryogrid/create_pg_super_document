# convertTSFunction

## Location
[src/bin/pg_dump/pg_dump.c:13252-13273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L13252-L13273)

## Overview
Converts a function OID obtained from text search parsers or templates into a properly formatted function name string using PostgreSQL's REGPROC type.

## Definition


## Detailed Description
This function takes a function OID and converts it to a human-readable function name by executing a SQL query that casts the OID to the REGPROC type. The REGPROC type automatically resolves the OID to the function's name. Since text search parser and template functions have predetermined argument lists, using REGPROC (which doesn't include argument types) is sufficient rather than REGPROCEDURE.

The function executes a SQL query to perform the conversion and returns the result as a dynamically allocated string. The conversion is search path dependent, so the caller must ensure the proper schema context.

## Parameters / Member Variables
- : Archive connection handle for executing SQL queries
- : The OID of the function to convert to a name string

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (for query formatting)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (to execute the conversion query)
  - [pg_strdup](../p/pg_strdup.md) (for string duplication)
  - [PQgetvalue](../P/PQgetvalue.md) (to extract query result)
  - [PQclear](../P/PQclear.md) (to free query result)
- Called from (representative examples):
  - [dumpTSParser](../d/dumpTSParser.md) (multiple calls for different parser functions)
  - [dumpTSTemplate](../d/dumpTSTemplate.md) (for template functions)
  - fmtQualifiedDumpable

## Notes and Other Information
- Uses REGPROC rather than REGPROCEDURE since text search function argument lists are predetermined
- Results are search path dependent - caller must ensure proper schema context
- The returned string must be freed by the caller
- Specifically designed for text search parser and template function OID conversion
- Part of PostgreSQL's pg_dump utility for handling text search configuration dumps
- Executes a live SQL query against the database to perform the OID-to-name conversion