# tsvectorout

## Location
src/backend/utils/adt/tsvector.c: 314 - 406

## Overview
The  function converts a PostgreSQL TSVector data type to its textual string representation for output and display purposes.

## Definition


## Detailed Description
This function is the output function for the TSVector data type, which serializes a TSVector's internal binary representation into a human-readable string format. The function handles the complex task of formatting lexemes along with their positional information and weights. Each lexeme is enclosed in single quotes and properly escaped, with positions and weights (A, B, C, D) displayed after a colon when present. The output format follows the pattern: .

The function carefully calculates the required buffer size to accommodate all lexemes, their positions, weights, and necessary escape characters. It processes each word entry in the TSVector, properly escaping single quotes and backslashes within lexeme text, and formats positional data with corresponding weight letters (A=weight 3, B=weight 2, C=weight 1, D or no letter=weight 0).

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing the TSVector input parameter

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extract TSVector from function arguments
  - ARRPTR: Get pointer to word entries array
  - STRPTR: Get pointer to string data
  - POSDATAPTR: Get pointer to position data
  - POSDATALEN: Get length of position data
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md): Get maximum character length for encoding
  - [pg_mblen](../p/pg_mblen.md): Get multibyte character length
  - t_iseq: Test character equality
  - WEP_GETPOS: Extract position from word entry position data
  - WEP_GETWEIGHT: Extract weight from word entry position data
  - PG_RETURN_CSTRING: Return C string result
- Called from (representative examples):
  - PostgreSQL type system for TSVector output operations
  - SQL queries requiring TSVector text representation

## Notes and Other Information
- The function properly handles multibyte character encodings
- Memory allocation is carefully calculated to prevent buffer overruns
- Single quotes and backslashes within lexemes are properly escaped by doubling them
- Position weights are represented as letters: A (highest), B, C, D (or omitted for lowest)
- The output format is compatible with tsvector input parsing functions