# utf8_to_euc_jp

## Location
src/backend/utils/mb/conversion_procs/utf8_and_euc_jp/utf8_and_euc_jp.c: 60 - 78

## Overview
Converts character encoding from UTF-8 to EUC-JP (Extended Unix Code for Japanese), performing multibyte character set conversion for Japanese text data.

## Definition
```c
Datum utf8_to_euc_jp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL conversion procedure that transforms text encoded in UTF-8 format to EUC-JP encoding. It serves as the reverse counterpart to euc_jp_to_utf8, enabling conversion from Unicode UTF-8 back to the Japanese EUC encoding system. The function uses PostgreSQL's internal conversion framework and leverages a Unicode mapping tree (`euc_jp_from_unicode_tree`) to perform accurate character-by-character conversion.

The conversion process involves parsing the source UTF-8 encoded string, mapping each Unicode code point through the conversion tree, and generating the corresponding EUC-JP encoded output. The function handles both single-byte ASCII characters and multibyte Japanese characters according to both UTF-8 and EUC-JP specifications.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument structure containing:
  - `src` (unsigned char *): Source string in UTF-8 encoding (extracted from arg 2)
  - `dest` (unsigned char *): Destination buffer for EUC-JP output (extracted from arg 3)  
  - `len` (int): Length of the source string in bytes (extracted from arg 4)
  - `noError` (bool): Flag to suppress conversion errors if true (extracted from arg 5)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING`: Extract C string arguments from PostgreSQL function call
  - `PG_GETARG_INT32`: Extract integer arguments from PostgreSQL function call
  - `PG_GETARG_BOOL`: Extract boolean arguments from PostgreSQL function call
  - `CHECK_ENCODING_CONVERSION_ARGS`: Validate encoding conversion arguments
  - `UtfToLocal`: Core conversion function that performs the actual encoding transformation
  - `PG_RETURN_INT32`: Return integer result to PostgreSQL
- Constants referenced:
  - `PG_UTF8`: PostgreSQL encoding identifier for UTF-8
  - `PG_EUC_JP`: PostgreSQL encoding identifier for EUC-JP
  - `euc_jp_from_unicode_tree`: Character mapping tree for Unicode to EUC-JP conversion
- Called from (representative examples):
  - No direct callers found (likely registered as a conversion procedure in the system catalogs)

## Notes and Other Information
- This function is typically registered as a conversion procedure in PostgreSQL's system catalogs rather than being called directly
- The function returns the number of bytes successfully converted as an integer
- Error handling is controlled by the `noError` parameter - when true, conversion failures are silently handled rather than throwing exceptions
- The conversion relies on pre-built mapping trees that contain the character correspondence between Unicode and EUC-JP code points
- Some Unicode characters may not have equivalent representations in EUC-JP, which could result in conversion errors or substitutions
- Located in: `src/backend/utils/mb/conversion_procs/utf8_and_euc_jp/utf8_and_euc_jp.c:60-78`