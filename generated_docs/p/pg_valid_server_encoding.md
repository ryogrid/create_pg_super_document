# pg_valid_server_encoding

## Location
[src/common/encnames.c:499-512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/encnames.c#L499-L512)

## Overview
Validates whether a given encoding name is a valid server-side character encoding in PostgreSQL.

## Definition
int pg_valid_server_encoding(const char *name)

## Detailed Description
This function validates an encoding name string to determine if it represents a valid server-side character encoding. It performs a two-step validation process: first converting the encoding name to an internal encoding identifier using pg_char_to_encoding, then checking if the resulting encoding is valid for backend (server) use with the PG_VALID_BE_ENCODING macro. Server encodings are those that can be used by PostgreSQL databases and server-side operations.

## Parameters / Member Variables
- name: String containing the name of the character encoding to validate

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_to_encoding (converts encoding name string to internal encoding ID)
  - PG_VALID_BE_ENCODING (macro for validating backend/server encodings)
- Called from (representative examples):
  - [createdb](../c/createdb.md) (src/backend/commands/dbcommands.c:883, 893)
  - [parse_extension_control_file](parse_extension_control_file.md) (src/backend/commands/extension.c:590)
  - [get_encoding_id](../g/get_encoding_id.md) (src/bin/initdb/initdb.c:849)

## Notes and Other Information
- Returns the encoding ID (positive integer) if valid, or -1 if invalid
- [Backend](../B/Backend.md) encodings are those suitable for server-side database storage and operations
- Used for validating database encoding settings during CREATE DATABASE operations
- Also used by initdb and extension loading to validate encoding specifications
- Located in src/common/encnames.c:499-512