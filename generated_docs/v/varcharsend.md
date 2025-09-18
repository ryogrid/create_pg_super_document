# varcharsend

## Location
[src/backend/utils/adt/varchar.c:548-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L548-L564)

## Overview
Converts a VARCHAR value to binary format for transmission, delegating the actual work to the text type's send function.

## Definition


## Detailed Description
The `varcharsend` function serves as the binary output function for the VARCHAR data type in PostgreSQL. It converts a VARCHAR value to PostgreSQL's external binary format for transmission over the wire protocol. Rather than implementing separate logic, this function leverages the fact that VARCHAR and text types have identical internal representations by directly calling `textsend` to perform the conversion.

This function is part of PostgreSQL's type I/O system and is called when VARCHAR data needs to be sent in binary format, such as during COPY operations with binary format, or when using the binary protocol for query results.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (accessed as `fcinfo`)
  - Argument 0: The VARCHAR datum to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - [textsend](../t/textsend.md): Performs the actual conversion of text data to binary format

- Called from (representative examples):
  - PostgreSQL binary protocol output handlers
  - COPY command with binary format
  - Binary result set transmission

## Notes and Other Information
- This function is essentially a wrapper around `textsend`, demonstrating the equivalence of VARCHAR and text types in PostgreSQL
- The binary format output is optimized for network transmission and storage
- Part of PostgreSQL's standard type I/O function set registered in system catalogs
- The delegation to `textsend` ensures consistency and reduces code duplication
- Used specifically for binary protocol output, complementing `varcharout` which handles text protocol output