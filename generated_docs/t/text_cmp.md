# text_cmp

## Location
[src/backend/utils/adt/varlena.c:1594-1618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1594-L1618)

## Overview
Internal comparison function that extracts variable-length string data from PostgreSQL text objects and delegates to varstr_cmp for actual comparison logic.

## Definition


## Detailed Description
`text_cmp` serves as a wrapper function that handles PostgreSQL's text data type comparison by extracting the raw string data and lengths from text objects, then calling `varstr_cmp` to perform the actual comparison. This function abstracts the variable-length data extraction process, using VARDATA_ANY and VARSIZE_ANY_EXHDR macros to handle both short-header and long-header varlena formats transparently.

## Parameters / Member Variables
- `arg1`: First text object to compare
- `arg2`: Second text object to compare  
- `collid`: OID of the collation to use for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [varstr_cmp](../v/varstr_cmp.md)
  - VARDATA_ANY (macro)
  - VARSIZE_ANY_EXHDR (macro)
- Called from (representative examples):
  - [texteq](texteq.md)
  - [textne](textne.md)
  - [text_lt](text_lt.md)
  - [text_le](text_le.md)
  - [text_gt](text_gt.md)
  - [text_ge](text_ge.md)
  - [bttextcmp](../b/bttextcmp.md)

## Notes and Other Information
- Static function serving as internal implementation detail
- Handles varlena data format abstraction for text comparison operations
- Returns standard comparison result: -1 (less), 0 (equal), or 1 (greater)
- Central hub for all PostgreSQL text comparison operators and functions