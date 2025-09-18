# euc_cn_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_euc_cn/utf8_and_euc_cn.c:39-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_euc_cn/utf8_and_euc_cn.c#L39-L59)

## Overview
A PostgreSQL conversion function that converts text from EUC_CN (Extended Unix Code for Chinese) encoding to UTF-8 encoding.

## Definition
```c
Datum euc_cn_to_utf8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms text data from EUC_CN encoding (a variable-width character encoding for Chinese text) to UTF-8 encoding. It is implemented as a PostgreSQL internal function that follows the standard conversion procedure interface, taking source and destination buffers along with conversion parameters and returning the number of bytes successfully converted.

The function utilizes the `LocalToUtf` conversion utility with the `euc_cn_to_unicode_tree` conversion table to perform the actual character encoding transformation. It includes proper error handling through the `noError` parameter and validates encoding compatibility using the `CHECK_ENCODING_CONVERSION_ARGS` macro.

## Parameters / Member Variables
- `PG_GETARG_CSTRING(2)`: Source string in EUC_CN encoding (null-terminated C string)
- `PG_GETARG_CSTRING(3)`: Destination buffer for UTF-8 encoded output (null-terminated C string)
- `PG_GETARG_INT32(4)`: Length of the source string in bytes
- `PG_GETARG_BOOL(5)`: Error handling flag - if true, don't throw an error if conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - CHECK_ENCODING_CONVERSION_ARGS
  - [LocalToUtf](../L/LocalToUtf.md)
  - PG_RETURN_INT32
- Constants used:
  - PG_UTF8
  - PG_EUC_CN
  - euc_cn_to_unicode_tree
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL's conversion system)

## Notes and Other Information
- This function is part of PostgreSQL's multi-byte character set conversion system
- The function is designed to be called internally by PostgreSQL's encoding conversion framework
- Returns the number of bytes successfully converted as a 32-bit integer
- The conversion uses a pre-built Unicode mapping tree (`euc_cn_to_unicode_tree`) for efficient character mapping
- Error handling is controlled by the `noError` parameter, allowing for graceful handling of conversion failures
- Located in the conversion procedures module specifically for UTF-8 and EUC_CN conversions