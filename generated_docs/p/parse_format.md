# parse_format

## Location
[src/backend/utils/adt/formatting.c:1328-1475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1328-L1475)

## Overview
Main format parser that processes format strings and builds a FormatNode tree by identifying keywords, suffixes, and separators for both date-time and numeric formatting.

## Definition
```c
static void
parse_format(FormatNode *node, const char *str, const KeyWord *kw,
             const KeySuffix *suf, const int *index, uint32 flags, NUMDesc *Num)
```

## Detailed Description
This comprehensive format parser processes input format strings and constructs a structured FormatNode tree representation. It handles both date-time (DCH) and numeric (NUM) format parsing with support for keywords, prefixes, postfixes, separators, quoted strings, and special characters.

The parser operates in several phases for each character position:
1. **Prefix Detection**: Searches for format prefixes using suffix search
2. **Keyword Recognition**: Uses index-based sequential search to identify format keywords
3. **Postfix Processing**: Identifies format postfixes after keywords
4. **Character Handling**: Processes separators, spaces, quoted strings, and literal characters

Key parsing features:
- **Standard Mode**: Restricts separators to "-./,':; " with validation
- **Quoted String Processing**: Handles double-quoted literal strings with backslash escaping  
- **Separator Classification**: Distinguishes between separators, spaces, and regular characters
- **Numeric Preparation**: Integrates with NUMDesc_prepare for numeric format validation
- **Multi-byte Character Support**: Uses pg_mblen for proper character boundary handling

The function builds a linked sequence of FormatNode structures, each representing a format element (keyword, separator, character, etc.) with associated type, suffix flags, and character data.

## Parameters / Member Variables
- `node`: Array of FormatNode structures to populate with parsed format elements
- `str`: Input format string to parse
- `kw`: Array of KeyWord structures for keyword lookup
- `suf`: Array of KeySuffix structures for prefix/postfix lookup  
- `index`: Index array for optimized keyword search
- `flags`: Formatting flags (DCH_FLAG, NUM_FLAG, STD_FLAG) controlling parser behavior
- `Num`: Pointer to NUMDesc structure for numeric format preparation (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [suff_search](../s/suff_search.md) (for prefix/postfix lookup)
  - [index_seq_search](../i/index_seq_search.md) (for keyword identification)
  - [NUMDesc_prepare](../N/NUMDesc_prepare.md) (for numeric format processing)
  - [is_separator_char](../i/is_separator_char.md) (for separator character detection)
  - [pg_mblen](pg_mblen.md) (for multi-byte character length)
  - [pnstrdup](pnstrdup.md) (for string duplication in error messages)
  - Various NODE_TYPE_* constants and flag macros
- Called from (representative examples):
  - DCH_ZONED
  - [DCH_cache_fetch](../D/DCH_cache_fetch.md)  
  - [datetime_to_char_body](../d/datetime_to_char_body.md)
  - [datetime_format_has_tz](../d/datetime_format_has_tz.md)
  - [do_to_timestamp](../d/do_to_timestamp.md)
  - [NUM_cache_fetch](../N/NUM_cache_fetch.md)
  - [NUM_cache](../N/NUM_cache.md)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Supports both date-time and numeric format parsing through flag-based mode selection
- Handles complex quoting rules: backslash escapes work inside quotes, backslash only escapes quotes outside
- Standard mode enforces restricted separator set for datetime compatibility
- The parser terminates format node array with NODE_TYPE_END
- Integrates error reporting for invalid datetime format separators in standard mode
- Multi-byte character aware for international character support
- Central component of PostgreSQL's format string processing system