# pg_valid_server_encoding_id

## Location
[src/common/encnames.c:513-523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/encnames.c#L513-L523)

## Overview
Validates whether a given encoding identifier is a valid server-side character encoding in PostgreSQL.

## Definition
int pg_valid_server_encoding_id(int encoding)

## Detailed Description
This function provides a simple wrapper around the PG_VALID_BE_ENCODING macro to validate whether an encoding identifier represents a valid backend (server-side) character encoding. Unlike pg_valid_server_encoding which takes an encoding name string, this function takes the numeric encoding identifier directly and returns the result of the validation check.

## Parameters / Member Variables
- encoding: Integer identifier representing a PostgreSQL character encoding (from the pg_enc enum)

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_BE_ENCODING (macro for validating backend/server encodings)
- Called from (representative examples):
  - [setup_locale_encoding](../s/setup_locale_encoding.md) (src/bin/initdb/initdb.c:2717)
  - PQnoPasswordSupplied (src/interfaces/libpq/libpq-fe.h:723)

## Notes and Other Information
- Returns non-zero (true) if the encoding is valid for server use, zero (false) otherwise
- This is a convenience function that directly exposes the PG_VALID_BE_ENCODING macro functionality
- Used when the encoding ID is already known and only validation is needed
- More efficient than pg_valid_server_encoding when working with encoding IDs rather than names
- Located in src/common/encnames.c:513-523