# UtfToLocal

## Location
src/backend/utils/mb/conv.c: 507 - 716

## Overview
A comprehensive UTF-8 to local encoding conversion function that handles multibyte character conversion using multiple lookup strategies including combined character mappings, radix trees, and algorithmic conversion functions.

## Definition
```c
int UtfToLocal(const unsigned char *utf, int len, unsigned char *iso, const pg_mb_radix_tree *map, const pg_utf_to_local_combined *cmap, int cmapsize, utf_local_conversion_func conv_func, int encoding, bool noError)
```

## Detailed Description
The `UtfToLocal` function is the primary UTF-8 to local encoding conversion routine in PostgreSQL. It processes a UTF-8 encoded input string and converts it to a specified local encoding using a sophisticated multi-tiered approach. The function employs three conversion strategies in order of preference:

1. **Combined character mapping (cmap)**: Used for sequences where multiple UTF-8 characters combine to form a single local character
2. **Radix tree mapping (map)**: Efficient single-character conversion using a prefix tree structure
3. **Algorithmic conversion (conv_func)**: Custom conversion functions for specific encoding pairs

The function handles variable-length UTF-8 characters (1-4 bytes) and performs comprehensive validation of input data. For ASCII characters (single byte), it applies a fast-path optimization assuming one-to-one conversion. For multibyte characters, it systematically attempts each conversion method until a match is found.

Special handling is provided for combined characters, where the function looks ahead to process multi-character UTF-8 sequences that may map to a single character in the target encoding. The function includes robust error handling and can operate in both strict mode (raising errors for untranslatable characters) and lenient mode (stopping at the first untranslatable character).

## Parameters / Member Variables
- `utf`: Input string in UTF-8 encoding (need not be null-terminated)
- `len`: Length of input string in bytes
- `iso`: Pointer to output area (must be large enough, output will be null-terminated)
- `map`: Conversion radix tree for single character mappings
- `cmap`: Optional conversion map for combined characters (can be NULL)
- `cmapsize`: Number of entries in the combined character conversion map (0 if cmap is NULL)
- `conv_func`: Optional algorithmic encoding conversion function (can be NULL)
- `encoding`: PostgreSQL identifier for the target local encoding
- `noError`: If true, stops conversion at first untranslatable character instead of raising error

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING (encoding validation macro)
  - pg_utf_mblen (UTF-8 character length determination)
  - [pg_utf8_islegal](../p/pg_utf8_islegal.md) (UTF-8 sequence validation)
  - [compare3](../c/compare3.md) (comparison function for combined character bsearch)
  - [pg_mb_radix_conv](../p/pg_mb_radix_conv.md) (radix tree character conversion)
  - [store_coded_char](../s/store_coded_char.md) (multibyte character output formatting)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (error reporting for invalid UTF-8)
  - [report_untranslatable_char](../r/report_untranslatable_char.md) (error reporting for conversion failures)
- Called from (representative examples):
  - [utf8_to_big5](../u/utf8_to_big5.md) (Big5 encoding conversion)
  - [utf8_to_euc_jp](../u/utf8_to_euc_jp.md) (EUC-JP encoding conversion)
  - [utf8_to_iso8859](../u/utf8_to_iso8859.md) (ISO-8859 encoding conversion)
  - [utf8_to_win](../u/utf8_to_win.md) (Windows code page conversion)
  - [Many other encoding-specific conversion functions]

## Notes and Other Information
- Returns the number of input bytes successfully consumed
- The output string is always null-terminated
- Supports both strict error handling and graceful degradation modes
- Critical component of PostgreSQLs multibyte character encoding infrastructure
- Handles complex scenarios like combining characters and surrogate pairs
- Performance-optimized with ASCII fast-path and efficient lookup structures
- Used extensively by all UTF-8 to local encoding conversion procedures in PostgreSQL