# esc_dec_len

## Location
src/backend/utils/adt/encode.c: 523 - 602

## Overview
Calculates the length of the decoded byte array that would result from decoding an escape-encoded string representation.

## Definition

```c
struct
{
	const char *name;
	struct pg_encoding enc;
}			enclist[] =

{
	{
		"hex",
		{
			hex_enc_len, hex_dec_len, hex_encode, hex_decode
		}
	},
	{
		"base64",
		{
			pg_base64_enc_len, pg_base64_dec_len, pg_base64_encode, pg_base64_decode
		}
	},
	{
		"escape",
		{
			esc_enc_len, esc_dec_len, esc_encode, esc_decode
		}
	},
	{
		NULL,
		{
			NULL, NULL, NULL, NULL
		}
	}
};
```
## Detailed Description
This function analyzes an escape-encoded string and determines how many bytes the decoded output would contain. It parses escape sequences used in PostgreSQL's escape encoding format for bytea data types. The function handles three types of sequences:

1. Regular characters (not backslash) - each counts as one byte
2. Octal escape sequences (\### where each # is 0-7) - each sequence represents one byte
3. Double backslash (\\) - represents a single backslash byte

If an invalid escape sequence is encountered (a backslash not followed by valid octal digits or another backslash), the function raises an error.

## Parameters / Member Variables
- : Pointer to the escape-encoded input string to analyze
- : Length in bytes of the input string

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - ERROR (error level constant)
  - errcode (for error code specification)
  - ERRCODE_INVALID_TEXT_REPRESENTATION (specific error code)
  - errmsg (for error message formatting)
- Called from (representative examples):
  - Used in the escape encoding system as part of the enclist structure
  - Part of PostgreSQL's bytea encoding/decoding infrastructure

## Notes and Other Information
- This is a static function, only accessible within the encode.c file
- Returns uint64 to handle potentially large decoded lengths
- The function performs validation during length calculation, throwing errors for invalid escape sequences
- Used as part of PostgreSQL's escape encoding scheme for bytea data types
- The function is referenced in the enclist array alongside esc_enc_len, esc_encode, and esc_decode