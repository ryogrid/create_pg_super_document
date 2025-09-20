# pq_buffer_remaining_data

## Location
[src/backend/libpq/pqcomm.c:1126-1139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1126-L1139)

## Overview
Returns the number of bytes remaining in the PostgreSQL communication receive buffer without attempting to read more data from the network.

## Definition

```c
ssize_t
pq_buffer_remaining_data(void)
```
## Detailed Description
 is a utility function that calculates and returns the number of bytes currently available in the receive buffer that have not yet been consumed. This function is designed to be non-blocking and will not attempt to read additional data from the network connection.

The function performs a simple calculation: , which represents the difference between the total length of data in the buffer and the current read position. This gives the exact number of bytes that can be read without triggering additional network I/O operations.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - : Global variable indicating total length of data in receive buffer  
  - : Global variable indicating current read position in receive buffer
- Called from (representative examples):
  - : Used during secure connection establishment
  - : Used during backend startup packet processing
  - : Used in frontend/backend event management

## Notes and Other Information
- This is a non-static function, accessible from other modules through libpq.h
- Returns  type to handle potentially large buffer sizes
- Includes an assertion that  to ensure buffer integrity
- Guarantees that reading up to the returned number of bytes will not cause additional network reads
- Commonly used for determining buffer state before making read decisions in connection handling