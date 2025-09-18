# pqPutMsgBytes

## Location
src/interfaces/libpq/fe-misc.c: 494 - 516

## Overview
Adds raw bytes to a partially-constructed message in the PostgreSQL libpq client library's output buffer.

## Definition


## Detailed Description
The  function is a low-level utility in the libpq client library that appends raw byte data to the output buffer for a message being constructed. It ensures there is sufficient buffer space before copying the data and updates the message end position. This function is fundamental to building PostgreSQL protocol messages on the client side.

The function performs buffer space validation using  and then uses  to efficiently copy the data into the output buffer. It maintains the connection's  pointer to track where the current message ends.

## Parameters / Member Variables
- : Pointer to the source data buffer containing bytes to be added to the message
- : Number of bytes to copy from the source buffer
- : PostgreSQL connection object containing the output buffer and state information

## Dependencies
- Functions called/Symbols referenced:
  - [pqCheckOutBufferSpace](pqCheckOutBufferSpace.md)
  - memcpy (standard library)
- Called from (representative examples):
  - [pqPutc](pqPutc.md)
  - [pqPuts](pqPuts.md)
  - [pqPutnchar](pqPutnchar.md)
  - [pqPutInt](pqPutInt.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the fe-misc.c compilation unit
- Returns 0 on success, EOF on error (specifically when buffer space allocation fails)
- The caller is responsible for Pfdebug calls as noted in the source comment
- This function is part of the message construction layer in the PostgreSQL wire protocol implementation
- The function directly manipulates the connection's output buffer () and message end position ()