# vacuum_error_callback

## Location
[src/backend/access/heap/vacuumlazy.c:3106-3169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L3106-L3169)

## Overview
Error context callback function that provides detailed error messages during vacuum operations, specifying the exact phase, location, and context where errors occur.

## Definition

```c
static void
vacuum_error_callback(void *arg)
```
## Detailed Description
The  function serves as an error context callback specifically designed for vacuum operations. It receives an  structure containing error information and generates contextual error messages based on the current vacuum phase and location where the error occurred. The function provides detailed error context messages that match those used in parallel vacuum operations, ensuring consistency across both serial and parallel vacuum implementations.

The function handles multiple vacuum phases including heap scanning, heap vacuuming, index vacuuming, index cleanup, and relation truncation. For each phase, it provides specific error context messages that include relevant details such as block numbers, offset numbers, relation names, and index names as appropriate.

## Parameters / Member Variables
- : A void pointer to an  structure containing error information including phase, block number, offset number, relation name, relation namespace, and index name

## Dependencies
- Functions called/Symbols referenced:
  -  (error info structure)
  -  (block number validation)
  -  (offset number validation) 
  -  (error context reporting)
  -  (vacuum phase constant)
  -  (vacuum phase constant)
  -  (vacuum phase constant)
  -  (vacuum phase constant)
  -  (vacuum phase constant)
  -  (vacuum phase constant)
- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:352)

## Notes and Other Information
- This function is designed to work in conjunction with  and their error messages should be kept in sync
- The function handles various vacuum phases with appropriate context messages for each
- For unknown or uninitialized phases, the function returns without setting any error context
- Error messages include specific location information (block numbers, offset numbers) when available
- The function is static and only used within the vacuumlazy.c module