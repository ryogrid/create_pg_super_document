# multirange_send

## Location
[src/backend/utils/adt/multirangetypes.c:377-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L377-L415)

## Overview
Serializes a PostgreSQL multirange value to its binary representation for network transmission or storage, encoding the range count and individual range data.

## Definition


## Detailed Description
The  function is the binary output function for PostgreSQL multirange types, responsible for converting internal multirange format into binary wire format. The binary representation it produces consists of:

1. Standard PostgreSQL type send header (via pq_begintypsend)
2. A 4-byte integer count of ranges in the multirange
3. For each range:
   - A 4-byte length indicator (excluding TOAST header)
   - The range data in binary format (as produced by the range type's send function)
4. Standard PostgreSQL type send footer (via pq_endtypsend)

The function first deserializes the multirange into its constituent ranges, then iterates through each range. For each range, it:
- Calls the range type's send function to get the binary representation
- Writes the length of the binary data (excluding VARLENA header)
- Writes the actual binary range data

This binary format is platform-independent and suitable for network transmission between PostgreSQL instances.

## Parameters / Member Variables
- : The multirange value to serialize to binary format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeTypeGetOid
  - makeStringInfo
  - [get_multirange_io_data](../g/get_multirange_io_data.md)
  - IOFunc_send
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [multirange_deserialize](multirange_deserialize.md)
  - RangeTypePGetDatum
  - [SendFunctionCall](../S/SendFunctionCall.md)
  - pq_sendbytes
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - PG_RETURN_BYTEA_P
- Called from:
  - PostgreSQL type system (send function registration)

## Notes and Other Information
- Uses PostgreSQL's standard binary protocol message building (pq_send* functions)
- The range count is stored directly from the multirange's rangeCount field
- Individual ranges are serialized using their respective send functions
- VARLENA header handling ensures proper binary data transmission
- The output format is exactly what multirange_recv expects as input
- Follows PostgreSQL's standard binary type serialization protocol
- Returns a bytea value containing the complete binary representation