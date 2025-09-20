# pq_discardbytes

## Location
[src/backend/libpq/pqcomm.c:1096-1125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1096-L1125)

## Overview
Discards a specified number of bytes from the PostgreSQL communication receive buffer without copying the data, primarily used for resynchronization after read errors.

## Definition

```c
static int
pq_discardbytes(size_t len)
```
## Detailed Description
 is a low-level function that throws away a known number of bytes from the receive buffer. Unlike , this function does not copy the discarded data to any destination. The primary purpose is to facilitate resynchronization after read errors by advancing the receive pointer past unwanted or corrupted data.

The function operates by:
1. Ensuring data is available in the receive buffer (calling  if needed)
2. Calculating how many bytes can be discarded from the current buffer position
3. Advancing the receive pointer () by the amount discarded
4. Repeating until all requested bytes are discarded

## Parameters / Member Variables
- : The number of bytes to discard from the receive buffer

## Dependencies
- Functions called/Symbols referenced:
  - : Called to receive more data when buffer is empty
  - : Global flag asserting message reading state
  - : Global pointer to current position in receive buffer
  - : Global variable indicating length of data in receive buffer
- Called from (representative examples):
  - : Uses this function for error recovery

## Notes and Other Information
- This is a static function, only accessible within the pqcomm.c module
- Returns 0 on success, EOF on failure (when  fails to receive data)
- The function includes an assertion that  is true, ensuring it's only called during message reading
- Essential for error handling and protocol recovery in PostgreSQL's communication layer