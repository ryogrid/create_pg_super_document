# varcharrecv

## Location
[src/backend/utils/adt/varchar.c:527-547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L527-L547)

## Overview
Converts external binary format data to a VARCHAR value, serving as the binary input function for the VARCHAR data type.

## Definition


## Detailed Description
The `varcharrecv` function handles the conversion of VARCHAR data from PostgreSQL's external binary format to the internal VARCHAR representation. This function is part of PostgreSQL's type I/O system and is called when receiving VARCHAR data through the binary protocol, such as during COPY operations with binary format or when using prepared statements with binary parameter formats.

The function extracts the string data from the input buffer, applies any type modifier constraints (such as length limits), and returns a properly formatted VARCHAR value. It uses the same input validation logic as the text input function through `varchar_input`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:
  - Argument 0: `StringInfo buf` - Input buffer containing the binary data
  - Argument 1: `Oid typelem` - Element type OID (not used, marked with NOT_USED)
  - Argument 2: `int32 atttypmod` - Type modifier specifying constraints like maximum length

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgtext](../p/pq_getmsgtext.md): Extracts text data from the binary message buffer
  - [varchar_input](varchar_input.md): Validates and converts the string to VARCHAR format
  - [pfree](../p/pfree.md): Frees the temporary string memory
  - `PG_RETURN_VARCHAR_P`: Returns the VARCHAR result

- Called from (representative examples):
  - PostgreSQL binary protocol handlers
  - COPY command with binary format
  - Prepared statement execution with binary parameters

## Notes and Other Information
- This function is specifically for binary protocol input, complementing `varcharin` which handles text protocol input
- The type element parameter is not used in the current implementation
- Memory management is handled carefully with `pfree` to avoid leaks
- The function respects type modifiers for length constraints through `varchar_input`
- Part of PostgreSQL's standard type I/O function set registered in system catalogs