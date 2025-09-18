# downcase_identifier

## Location
[src/backend/parser/scansup.c:46-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/scansup.c#L46-L92)

## Overview
The core workhorse function that performs case conversion of SQL identifiers with optional truncation, implementing PostgreSQL's identifier normalization rules.

## Definition


## Detailed Description
This function performs the actual work of converting SQL identifiers to lowercase following PostgreSQL's specific rules. It implements a hybrid downcasing approach to handle various character encodings properly: ASCII characters (A-Z) are converted using simple arithmetic, while high-bit characters use locale-aware  for single-byte encodings only.

The function handles the complexity of Unicode-aware case normalization by using a compromise approach. For 7-bit ASCII characters, it performs direct conversion to avoid locale-specific issues (such as Turkish locale problems with 'i' and 'I'). For characters with the high bit set, it uses  only when the database encoding is single-byte to avoid corrupting multi-byte character sequences.

If the resulting identifier length exceeds NAMEDATALEN and truncation is enabled, the function calls  to shorten it appropriately.

## Parameters / Member Variables
- : Pointer to the input identifier string (may not be null-terminated)
- : Length of the input identifier string in characters
- : Boolean flag indicating whether to emit warnings during processing
- : Boolean flag indicating whether to truncate long identifiers

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) (encoding information)
  - IS_HIGHBIT_SET (character testing macro)
  - isupper/tolower (character case functions)
  - [truncate_identifier](../t/truncate_identifier.md) (identifier truncation)
  - NAMEDATALEN (maximum identifier length constant)
- Called from (representative examples):
  - [downcase_truncate_identifier](downcase_truncate_identifier.md) (convenience wrapper)
  - [parse_ident](../p/parse_ident.md) (identifier parsing utility)

## Notes and Other Information
- Returns a newly 'd string that must be freed by the caller
- Designed to handle both null-terminated and non-null-terminated input strings via explicit length parameter
- Implements a careful balance between SQL99 Unicode-awareness and practical locale considerations
- The hybrid approach prevents corruption of multi-byte characters while providing reasonable case conversion
- Comments indicate future plans for full Unicode-aware case normalization when infrastructure becomes available
- Critical component of PostgreSQL's identifier processing pipeline in the parser subsystem