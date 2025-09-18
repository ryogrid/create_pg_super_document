# get_modifiers

## Location
[src/backend/utils/adt/tsquery.c:114-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L114-L164)

## Overview
A static helper function that parses modifier characters from tsquery strings to extract weight and prefix flag information.

## Definition


## Detailed Description
The get_modifiers function is a subroutine used in PostgreSQL's full-text search query parsing. It parses the modifiers part of a query token (like ':AB*' in a query) to extract weight information and prefix flag. The function processes characters following a colon (:) in the query string and sets appropriate weight bits for each valid weight character (A, B, C, D) and sets the prefix flag if an asterisk (*) is encountered.

The weight system uses a bitmask where:
- 'A' or 'a' sets bit 3 (weight 8)
- 'B' or 'b' sets bit 2 (weight 4)  
- 'C' or 'c' sets bit 1 (weight 2)
- 'D' or 'd' sets bit 0 (weight 1)

## Parameters / Member Variables
- : Input buffer containing the query string to parse
- : Output parameter that receives the combined weight bitmask
- : Output parameter that is set to true if prefix matching (*) is specified

## Dependencies
- Functions called/Symbols referenced:
  - t_iseq
  - [pg_mblen](../p/pg_mblen.md)
- Called from (representative examples):
  - [gettoken_query_standard](gettoken_query_standard.md)

## Notes and Other Information
- The function only processes single-byte characters for modifiers
- Weight characters are case-insensitive (both uppercase and lowercase accepted)
- The function stops parsing and returns when it encounters an unrecognized character
- This is part of PostgreSQL's full-text search infrastructure for parsing tsquery expressions