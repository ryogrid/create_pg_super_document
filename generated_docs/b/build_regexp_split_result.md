# build_regexp_split_result

## Location
[src/backend/utils/adt/regexp.c:1817-1857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1817-L1857)

## Overview
Builds the output string for the current match in regexp split operations, extracting the substring between consecutive pattern matches.

## Definition

```c
static Datum
build_regexp_split_result(regexp_matches_ctx *splitctx)
```
## Detailed Description
This static helper function constructs the result string for regexp split operations by extracting the substring between the current match and the previous match (or the beginning/end of the string for the first/last segments). It handles two different string representation modes: when a conversion buffer exists (for multi-byte character sets), it uses pg_wchar2mb_with_len for proper character encoding conversion; otherwise, it uses the more efficient text_substr function to extract the substring directly. The function calculates start and end positions from the match_locs array and includes error checking to ensure valid position values.

## Parameters / Member Variables
- : Pointer to regexp_matches_ctx structure containing:
  - : Conversion buffer for multi-byte character handling
  - : Wide character representation of the input string
  - : Original input string
  - : Array of match start/end positions  
  - : Index of current match being processed
  - : Size of conversion buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pg_wchar2mb_with_len](../p/pg_wchar2mb_with_len.md)
  - cstring_to_text_with_len
  - [text_substr](../t/text_substr.md)
  - DirectFunctionCall3
  - [PointerGetDatum](../P/PointerGetDatum.md), Int32GetDatum
  - [regexp_matches_ctx](../r/regexp_matches_ctx.md)
- Called from:
  - [regexp_split_to_table](../r/regexp_split_to_table.md)
  - [regexp_split_to_array](../r/regexp_split_to_array.md)

## Notes and Other Information
- Static function, not exposed outside regexp.c
- Handles both multi-byte and single-byte character encodings efficiently
- Includes validation to prevent invalid match positions
- Returns the substring between matches, not the matches themselves
- Uses different code paths for performance optimization based on character encoding needs
- Located at src/backend/utils/adt/regexp.c:1817-1857