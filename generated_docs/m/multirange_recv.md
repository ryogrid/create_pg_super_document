# multirange_recv

## Location
src/backend/utils/adt/multirangetypes.c: 337 - 376

## Overview
Deserializes a PostgreSQL multirange value from its binary representation, reconstructing the multirange from a count and sequence of binary-encoded ranges.

## Definition


## Detailed Description
The  function is the binary input function for PostgreSQL multirange types, responsible for converting binary wire format into internal multirange format. The binary representation consists of:

1. A 4-byte integer count of ranges in the multirange
2. For each range:
   - A 4-byte length indicator
   - The range data in binary format (as produced by the range type's send function)

The function reads the count, allocates an array for the ranges, then iterates through each range. For each range, it:
- Reads the range length
- Extracts the binary range data
- Creates a temporary StringInfo buffer containing the range data
- Calls the range type's receive function to deserialize the individual range
- Stores the resulting range in the array

Finally, it constructs and returns a multirange from the deserialized ranges.

## Parameters / Member Variables
- : StringInfo buffer containing the binary data to deserialize
- : OID of the multirange type being created
- : Type modifier for the multirange type

## Dependencies
- Functions called/Symbols referenced:
  - get_multirange_io_data
  - IOFunc_receive
  - pq_getmsgint
  - pq_getmsgbytes
  - resetStringInfo
  - appendBinaryStringInfo
  - ReceiveFunctionCall
  - DatumGetRangeTypeP
  - pq_getmsgend
  - make_multirange
  - PG_RETURN_MULTIRANGE_P
- Called from:
  - PostgreSQL type system (receive function registration)

## Notes and Other Information
- Uses PostgreSQL's standard binary protocol message handling (pq_getmsg* functions)
- Reuses a single temporary StringInfo buffer for efficiency when processing multiple ranges
- Each individual range is processed using the range type's own receive function
- Validates message format by calling pq_getmsgend() to ensure all data is consumed
- Memory is properly managed with pfree() for the temporary buffer
- The binary format is platform-independent and suitable for network transmission