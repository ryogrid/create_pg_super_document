# box_recv

## Location
src/backend/utils/adt/geo_ops.c: 466 - 500

## Overview
Converts external binary format data to the internal PostgreSQL BOX data type structure.

## Definition


## Detailed Description
The  function is a PostgreSQL binary input conversion function that deserializes a BOX structure from PostgreSQL's binary wire format. It reads four consecutive float8 values from a StringInfo buffer representing the coordinates of the box's high and low corners. Like , this function automatically reorders the coordinates to ensure canonical representation where the 'high' point contains maximum x and y values and the 'low' point contains minimum values. This function is used in PostgreSQL's binary protocol for efficient data transfer and storage.

## Parameters / Member Variables
- Uses  macro which provides:
  - : StringInfo buffer containing the binary data to be deserialized

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - [palloc](../p/palloc.md)
  - [pq_getmsgfloat8](../p/pq_getmsgfloat8.md)
  - [float8_lt](../f/float8_lt.md)
  - PG_RETURN_BOX_P
  - [BOX](../B/BOX.md) (struct type)
  - StringInfo (type)
- Called from (representative examples):
  - This is a PostgreSQL binary input function, typically called by the PostgreSQL protocol handler when receiving binary box data

## Notes and Other Information
This function follows PostgreSQL's standard binary receive function convention using the  interface. It is the binary counterpart to the text-based  function, providing more efficient data transfer for binary protocols. The function reads exactly four float8 values in the expected order: high.x, high.y, low.x, low.y. The coordinate normalization logic is identical to , ensuring consistent box representation regardless of input source. This function is essential for PostgreSQL's binary protocol support and is used in contexts such as prepared statements, COPY BINARY operations, and client-server communication using binary format.