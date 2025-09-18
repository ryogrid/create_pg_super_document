# PQescapeIdentifier

## Location
src/interfaces/libpq/fe-exec.c: 4371 - 4390

## Overview
PQescapeIdentifier escapes a string for use as a SQL identifier (table name, column name, etc.) in PostgreSQL queries, ensuring proper handling of special characters and reserved words.

## Definition


## Detailed Description
PQescapeIdentifier is a wrapper function that calls PQescapeInternal with the  parameter set to true, indicating that the string should be escaped as an identifier rather than a literal value. The function wraps the input string in double quotes and escapes any embedded double quotes by doubling them. Unlike literal escaping, identifier escaping does not require special handling of backslashes, as backslashes have no special meaning in PostgreSQL identifiers.

## Parameters / Member Variables
- : PostgreSQL connection handle used to determine client encoding and error reporting context
- : Input string to be escaped for use as a SQL identifier
- : Maximum length of the input string to process

## Dependencies
- Functions called/Symbols referenced:
  - [PQescapeInternal](PQescapeInternal.md)
- Called from (representative examples):
  - [stringlist_to_identifierstr](../s/stringlist_to_identifierstr.md) (in libpqwalreceiver.c)
  - [main](../m/main.md) (in pg_amcheck.c)
  - [create_publication](../c/create_publication.md) (in pg_createsubscriber.c)
  - [ddlinfo](../d/ddlinfo.md) (in pgbench.c)
  - [psql_get_variable](../p/psql_get_variable.md) (in psql/common.c)
  - PQchangePassword (in fe-auth.c)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Returns NULL on error (encoding violations, out of memory), with error details stored in the connection object
- Uses double quotes to delimit identifiers, following SQL standard conventions
- Escapes embedded double quotes by doubling them (""inside"" becomes '""inside""')
- Does not require escape string syntax since backslashes are not special in identifiers
- Essential for safely constructing dynamic SQL that references table names, column names, or other database objects
- Handles multibyte character validation to ensure proper encoding