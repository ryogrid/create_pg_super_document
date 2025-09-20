# euc_jp_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_euc_jp/utf8_and_euc_jp.c:39-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_euc_jp/utf8_and_euc_jp.c#L39-L59)

## Overview
Converts character encoding from EUC-JP (Extended Unix Code for Japanese) to UTF-8, performing multibyte character set conversion for Japanese text data.

## Definition

```c
Datum
euc_jp_to_utf8(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL conversion procedure that transforms text encoded in EUC-JP format to UTF-8 encoding. It serves as a bridge between the Japanese EUC encoding system and Unicode UTF-8, enabling proper handling of Japanese characters in PostgreSQL databases. The function uses PostgreSQL's internal conversion framework and leverages a Unicode mapping tree () to perform accurate character-by-character conversion.

The conversion process involves parsing the source EUC-JP encoded string, mapping each character through the Unicode conversion tree, and generating the corresponding UTF-8 encoded output. The function handles both single-byte ASCII characters and multibyte Japanese characters according to the EUC-JP specification.

## Parameters / Member Variables
- : PostgreSQL function argument structure containing:
  -  (unsigned char *): Source string in EUC-JP encoding (extracted from arg 2)
  -  (unsigned char *): Destination buffer for UTF-8 output (extracted from arg 3)  
  -  (int): Length of the source string in bytes (extracted from arg 4)
  -  (bool): Flag to suppress conversion errors if true (extracted from arg 5)

## Dependencies
- Functions called/Symbols referenced:
  - : Extract C string arguments from PostgreSQL function call
  - : Extract integer arguments from PostgreSQL function call
  - : Extract boolean arguments from PostgreSQL function call
  - : Validate encoding conversion arguments
  - : Core conversion function that performs the actual encoding transformation
  - : Return integer result to PostgreSQL
- Constants referenced:
  - : PostgreSQL encoding identifier for EUC-JP
  - : PostgreSQL encoding identifier for UTF-8
  - : Character mapping tree for EUC-JP to Unicode conversion
- Called from (representative examples):
  - No direct callers found (likely registered as a conversion procedure in the system catalogs)

## Notes and Other Information
- This function is typically registered as a conversion procedure in PostgreSQL's system catalogs rather than being called directly
- The function returns the number of bytes successfully converted as an integer
- Error handling is controlled by the  parameter - when true, conversion failures are silently handled rather than throwing exceptions
- The conversion relies on pre-built mapping trees that contain the character correspondence between EUC-JP and Unicode code points
- Located in: 