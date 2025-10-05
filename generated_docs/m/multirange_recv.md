# multirange_recv

## Location
[src/backend/utils/adt/multirangetypes.c:337-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L337-L376)

## Overview
Deserializes a PostgreSQL multirange value from its binary representation, reconstructing the multirange from a count and sequence of binary-encoded ranges.

## Definition

```c
struct output */
	pq_begintypsend(buf);
```
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
  - [get_multirange_io_data](../g/get_multirange_io_data.md)
  - IOFunc_receive
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [pq_getmsgbytes](../p/pq_getmsgbytes.md)
  - [resetStringInfo](../r/resetStringInfo.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [ReceiveFunctionCall](../R/ReceiveFunctionCall.md)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - [pq_getmsgend](../p/pq_getmsgend.md)
  - [make_multirange](make_multirange.md)
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

## Simplified Source

```c
Datum
multirange_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    Oid mltrngtypoid = PG_GETARG_OID(1);
    int32 typmod = PG_GETARG_INT32(2);

    // Get I/O cache for the multirange type
    MultirangeIOData *cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_receive);

    // Read count of ranges from binary data
    uint32 range_count = pq_getmsgint(buf, 4);
    RangeType **ranges = palloc(range_count * sizeof(RangeType *));

    // Temporary buffer for individual range data
    StringInfoData tmpbuf;
    initStringInfo(&tmpbuf);

    // Read each range from binary format
    for (int i = 0; i < range_count; i++) {
        // Read range length and data
        uint32 range_len = pq_getmsgint(buf, 4);
        const char *range_data = pq_getmsgbytes(buf, range_len);

        // Prepare temporary buffer with range data
        resetStringInfo(&tmpbuf);
        appendBinaryStringInfo(&tmpbuf, range_data, range_len);

        // Deserialize individual range using range type's receive function
        ranges[i] = DatumGetRangeTypeP(
            ReceiveFunctionCall(&cache->typioproc, &tmpbuf,
                              cache->typioparam, typmod));
    }

    pfree(tmpbuf.data);
    pq_getmsgend(buf);

    // Create multirange from deserialized ranges
    MultirangeType *ret = make_multirange(mltrngtypoid, cache->typcache->rngtype,
                                         range_count, ranges);
    PG_RETURN_MULTIRANGE_P(ret);
}
```