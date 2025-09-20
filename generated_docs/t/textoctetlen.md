# textoctetlen

## Location
[src/backend/utils/adt/varlena.c:731-749](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L731-L749)

## Overview
Returns the physical byte length of a text value, excluding the variable-length header.

## Definition

```c
Datum
textoctetlen(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function calculates and returns the actual number of bytes occupied by the text data portion of a PostgreSQL text value, excluding the VARHDRSZ (variable-length header size). This function provides an efficient way to determine the storage size of text data without needing to decompress or detoast the input value.

The function uses  to get the total size of the datum and then subtracts the variable header size to get just the data portion. This approach is efficient because it doesn't require detoasting the input, making it suitable for large text values that might be stored externally.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: A text datum whose byte length is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the total raw size of a datum
  - : Extracts datum argument from function call
  - : Returns 32-bit integer result
  - : Constant representing variable-length header size

- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- The function is optimized for performance by avoiding detoasting of the input
- Returns the byte length, not the character length (important distinction for multibyte encodings)
- The comment indicates this returns the physical length which is less than the VARSIZE of the text
- This function is useful for storage analysis and memory management operations