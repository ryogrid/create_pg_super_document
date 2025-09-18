# PQenv2encoding

## Location
[src/interfaces/libpq/fe-misc.c:1261-1279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1261-L1279)

## Overview
Retrieves the encoding ID from the PGCLIENTENCODING environment variable to determine the client character encoding.

## Definition
int PQenv2encoding(void)

## Detailed Description
PQenv2encoding reads the PGCLIENTENCODING environment variable and converts it to a PostgreSQL encoding ID. This function is used to determine the client-side character encoding based on the environment setting. If the environment variable is not set, empty, or contains an invalid encoding name, the function defaults to PG_SQL_ASCII encoding.

The function follows a simple validation process: it retrieves the environment variable value, validates it using pg_char_to_encoding, and falls back to a safe default if the encoding is invalid or not recognized.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - getenv (standard C library function)
  - pg_char_to_encoding
  - PG_SQL_ASCII (constant)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/psql/startup.c)
  - PQnoPasswordSupplied (referenced in src/interfaces/libpq/libpq-fe.h)

## Notes and Other Information
- The function always returns a valid encoding ID, never failing due to invalid input
- PG_SQL_ASCII is used as the fallback encoding when PGCLIENTENCODING is unset or invalid
- This function is part of libpq's client encoding detection mechanism
- The returned encoding ID can be used with other PostgreSQL encoding functions