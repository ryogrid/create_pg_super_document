# LocalToUtf

## Location
[src/backend/utils/mb/conv.c:717-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conv.c#L717-L838)

## Overview
LocalToUtf is a comprehensive character encoding conversion function that transforms text from various local encodings to UTF-8, supporting multiple conversion methods including radix trees, combined character maps, and algorithmic conversion functions.

## Definition

```c
int
LocalToUtf(const unsigned char *iso, int len,
		   unsigned char *utf,
		   const pg_mb_radix_tree *map,
		   const pg_local_to_utf_combined *cmap, int cmapsize,
		   utf_local_conversion_func conv_func,
		   int encoding,
		   bool noError)
```
## Detailed Description
LocalToUtf serves as the core conversion engine for transforming text from PostgreSQL's supported local encodings to UTF-8. The function implements a multi-tiered approach to character conversion:

1. **ASCII Fast Path**: Single-byte ASCII characters (0-127) are copied directly without conversion
2. **Radix Tree Lookup**: Uses a radix tree structure for efficient single character lookups
3. **Combined Character Map**: Handles characters that map to multiple UTF-8 codepoints using binary search
4. **Algorithmic Conversion**: Falls back to encoding-specific conversion functions for complex cases

The function processes multi-byte characters by first validating their structure using , then packing them into a 32-bit integer for lookup operations. If all conversion methods fail and  is false, the function reports translation errors.

## Parameters / Member Variables
- : Input string in local encoding (need not be null-terminated)
- : Length of input string in bytes
- : Pointer to output area (must be large enough, output will be null-terminated)
- : Conversion map for single characters using radix tree structure
- : Optional conversion map for combined characters that produce multiple UTF-8 codepoints
- : Number of entries in the combined character conversion map (0 if none)
- : Optional algorithmic encoding conversion function for complex cases
- : PostgreSQL identifier for the local encoding
- : If true, stops conversion on first error rather than reporting it

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING: Validates encoding identifier
  - IS_HIGHBIT_SET: Checks if byte has high bit set (non-ASCII)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md): Validates multibyte character structure
  - [pg_mb_radix_conv](../p/pg_mb_radix_conv.md): Performs radix tree character lookup
  - [store_coded_char](../s/store_coded_char.md): Stores UTF-8 encoded character to output buffer
  - [compare4](../c/compare4.md): Comparison function for binary search in combined character map
  - [report_untranslatable_char](../r/report_untranslatable_char.md): Reports characters that cannot be converted
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Reports invalid byte sequences in input
  
- Called from (representative examples):
  - [big5_to_utf8](../b/big5_to_utf8.md): Big5 to UTF-8 conversion
  - [koi8r_to_utf8](../k/koi8r_to_utf8.md): KOI8-R to UTF-8 conversion
  - [euc_jp_to_utf8](../e/euc_jp_to_utf8.md): EUC-JP to UTF-8 conversion
  - [sjis_to_utf8](../s/sjis_to_utf8.md): Shift-JIS to UTF-8 conversion
  - [iso8859_to_utf8](../i/iso8859_to_utf8.md): ISO 8859 family to UTF-8 conversion
  - [win_to_utf8](../w/win_to_utf8.md): Windows codepage to UTF-8 conversion

## Notes and Other Information
- Returns the number of input bytes consumed, which may be less than  if  is true and conversion fails
- The function handles characters of 1-4 bytes in length, packing them into a 32-bit integer for efficient lookup
- ASCII characters (0-127) bypass all conversion logic for optimal performance
- The three-tier conversion approach (radix tree → combined map → algorithmic function) provides flexibility for different encoding complexities
- Output buffer must be pre-allocated with sufficient space; the function does not perform bounds checking
- Used extensively throughout PostgreSQL's character encoding conversion system for consistent UTF-8 conversion across all supported encodings
- The function is critical for PostgreSQL's internationalization support, enabling proper text handling across different database encodings