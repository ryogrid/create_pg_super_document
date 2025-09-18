# PQescapeLiteral

## Location
src/interfaces/libpq/fe-exec.c: 4365 - 4370

## Overview
PQescapeLiteral escapes a string for use as a SQL literal value in PostgreSQL queries, ensuring proper handling of special characters and preventing SQL injection.

## Definition


## Detailed Description
PQescapeLiteral is a wrapper function that calls PQescapeInternal with the  parameter set to false, indicating that the string should be escaped as a literal value rather than an identifier. The function properly handles single quotes, backslashes, and multibyte characters according to the connection's client encoding. When backslashes are present, it uses PostgreSQL's escape string syntax (E'...') to ensure compatibility regardless of the standard_conforming_strings setting.

## Parameters / Member Variables
- : PostgreSQL connection handle used to determine client encoding and error reporting context
- : Input string to be escaped for use as a SQL literal
- : Maximum length of the input string to process

## Dependencies
- Functions called/Symbols referenced:
  - PQescapeInternal
- Called from (representative examples):
  - libpqrcv_startstreaming (in libpqwalreceiver.c)
  - check_and_drop_existing_subscriptions (in pg_createsubscriber.c)
  - create_logical_replication_slot (in pg_createsubscriber.c)
  - psql_get_variable (in psql/common.c)
  - PQchangePassword (in fe-auth.c)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Returns NULL on error (encoding violations, out of memory), with error details stored in the connection object
- Handles multibyte character validation to prevent encoding-based SQL injection attacks
- Automatically adds escape string syntax (E'...') when backslashes are present in literals
- Used extensively throughout PostgreSQL client tools and applications for safe SQL query construction