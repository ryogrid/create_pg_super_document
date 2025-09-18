# euc_tw_to_utf8

## Location
src/backend/utils/mb/conversion_procs/utf8_and_euc_tw/utf8_and_euc_tw.c: 39 - 59

## Overview
Converts character encoding from EUC-TW (Extended Unix Code for Taiwan) to UTF-8, serving as a PostgreSQL conversion procedure function.

## Definition


## Detailed Description
This function implements a PostgreSQL conversion procedure that transforms text encoded in EUC-TW format to UTF-8 encoding. It follows the standard PostgreSQL conversion procedure interface, accepting source and destination buffers along with conversion parameters. The function uses the LocalToUtf conversion utility with the EUC-TW to Unicode mapping tree to perform the actual character encoding transformation. It validates the encoding parameters and handles error conditions based on the noError flag.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (unsigned char*): Source string in EUC-TW encoding (extracted from arg 2)
  -  (unsigned char*): Destination buffer for UTF-8 output (extracted from arg 3) 
  -  (int): Length of source string in bytes (extracted from arg 4)
  -  (bool): If true, don't throw an error if conversion fails (extracted from arg 5)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [LocalToUtf](../L/LocalToUtf.md)
  - PG_RETURN_INT32
  - PG_EUC_TW (encoding constant)
  - PG_UTF8 (encoding constant)
  - euc_tw_to_unicode_tree (conversion mapping)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's encoding conversion system)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/utf8_and_euc_tw/utf8_and_euc_tw.c:39-59
- Returns the number of bytes successfully converted as an integer
- Part of PostgreSQL's multibyte character encoding conversion infrastructure
- Uses the euc_tw_to_unicode_tree mapping for character conversion
- Follows PostgreSQL's function calling conventions with PG_FUNCTION_ARGS interface
- EUC-TW is primarily used for Traditional Chinese text encoding in Taiwan