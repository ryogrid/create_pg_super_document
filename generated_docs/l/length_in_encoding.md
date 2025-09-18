# length_in_encoding

## Location
src/backend/utils/mb/mbutils.c: 615 - 643

## Overview
A PostgreSQL function that calculates the length of a string in characters when interpreted in a specified character encoding, while validating that the byte sequence is valid for that encoding.

## Definition


## Detailed Description
The `length_in_encoding` function is a SQL-callable function that takes a BYTEA string and an encoding name as parameters, then returns the character length of the string when interpreted in the specified encoding. The function performs encoding validation to ensure the byte sequence is valid for the given encoding and raises an error if invalid data is encountered.

This function is particularly useful for validating and measuring text data in different character encodings, especially when dealing with multi-byte character sets where byte length and character length may differ significantly.

## Parameters / Member Variables
- `string` (BYTEA): The input byte sequence to measure
- `src_encoding_name` (NAME): The name of the character encoding to use for interpretation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (macro for extracting BYTEA argument)
  - PG_GETARG_NAME (macro for extracting NAME argument)  
  - pg_char_to_encoding (converts encoding name to internal ID)
  - pg_verify_mbstr_len (validates encoding and returns character length)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Returns an INT4 value representing the character count
- Raises ERROR with ERRCODE_INVALID_PARAMETER_VALUE for unknown encoding names
- The function validates the entire byte sequence for encoding compliance
- Part of PostgreSQL's multi-byte character encoding support system
- Located in src/backend/utils/mb/mbutils.c:615-643