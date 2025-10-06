# range_recv

## Location
[src/backend/utils/adt/rangetypes.c:177-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L177-L260)

## Overview
The range_recv function is the binary receive function for PostgreSQL range types, responsible for deserializing range values from their binary wire format into internal RangeType structures.

## Definition

```c
structing RangeBound representation */
	lower.infinite = (flags & RANGE_LB_INF) != 0;
```
## Detailed Description
This function processes binary data received over the wire (typically from client connections) and reconstructs RangeType values from their serialized format. The binary format consists of a flags byte followed by the binary representations of the lower and upper bounds (when present). Each bound includes a 4-byte length header followed by the binary representation as produced by the element type's send function. The function handles flag validation, bound deserialization, and proper reconstruction of the RangeBound structures before creating the final canonicalized range.

## Parameters / Member Variables
-  (PG_GETARG_POINTER(0)): StringInfo buffer containing the binary data to deserialize
-  (PG_GETARG_OID(1)): The OID of the range type being processed
-  (PG_GETARG_INT32(2)): Type modifier for the range type

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Stack overflow protection for recursive calls
  - [get_range_io_data](../g/get_range_io_data.md): Retrieves I/O function cache data for the range type
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md): Reads a single byte from the message buffer
  - [pq_getmsgint](../p/pq_getmsgint.md): Reads a 4-byte integer from the message buffer
  - [pq_getmsgbytes](../p/pq_getmsgbytes.md): Reads a specified number of bytes from the message buffer
  - [pq_getmsgend](../p/pq_getmsgend.md): Validates that all message data has been consumed
  - [initStringInfo](../i/initStringInfo.md): Initializes a StringInfo buffer
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md): Appends binary data to a StringInfo buffer
  - [ReceiveFunctionCall](../R/ReceiveFunctionCall.md): Calls the element type's receive function
  - [pfree](../p/pfree.md): Frees allocated memory
  - [make_range](../m/make_range.md): Constructs and canonicalizes the final RangeType structure
- Data structures used:
  - [RangeIOData](../R/RangeIOData.md): Cache structure for I/O functions
  - RangeBound: Structure representing range boundaries
  - StringInfo/StringInfoData: Buffer structures for handling binary data
  - IOFunc_receive: Enum value for receive function type
- Macros used:
  - RANGE_HAS_LBOUND/RANGE_HAS_UBOUND: Check for boundary existence
  - RANGE_EMPTY: Empty range flag
  - RANGE_LB_INF/RANGE_UB_INF: Infinite boundary flags
  - RANGE_LB_INC/RANGE_UB_INC: Inclusive boundary flags
  - PG_RETURN_RANGE_P: Return macro for range types

## Notes and Other Information
- The binary format is well-documented: flags byte, then lower bound (if present), then upper bound (if present)
- Each bound is prefixed with a 4-byte length header followed by the binary representation
- The function performs flag validation by masking out unsupported flags, particularly RANGE_xB_NULL
- Temporary StringInfo buffers are used to pass binary data to the element type's receive function
- Memory management is handled carefully with pfree calls for temporary buffers
- The function includes stack depth checking for recursive deserialization of nested range types
- Canonicalization is performed through make_range to ensure consistent internal representation

## Simplified Source

```c
Datum
range_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    Oid range_type_oid = PG_GETARG_OID(1);
    int32 typmod = PG_GETARG_INT32(2);

    check_stack_depth(); // Guard against recursion

    // Get I/O cache for this range type
    RangeIOData *cache = get_range_io_data(fcinfo, range_type_oid, IOFunc_receive);

    // Read and validate flags
    char flags = pq_getmsgbyte(buf);
    flags &= (RANGE_EMPTY | RANGE_LB_INC | RANGE_LB_INF |
              RANGE_UB_INC | RANGE_UB_INF); // Mask unsupported flags

    RangeBound lower, upper;

    // Deserialize lower bound if present
    if (RANGE_HAS_LBOUND(flags)) {
        uint32 bound_len = pq_getmsgint(buf, 4);
        const char *bound_data = pq_getmsgbytes(buf, bound_len);

        // Create temporary buffer for bound data
        StringInfoData bound_buf;
        initStringInfo(&bound_buf);
        appendBinaryStringInfo(&bound_buf, bound_data, bound_len);

        lower.val = ReceiveFunctionCall(&cache->typioproc, &bound_buf,
                                        cache->typioparam, typmod);
        pfree(bound_buf.data);
    } else {
        lower.val = (Datum) 0;
    }

    // Deserialize upper bound if present
    if (RANGE_HAS_UBOUND(flags)) {
        uint32 bound_len = pq_getmsgint(buf, 4);
        const char *bound_data = pq_getmsgbytes(buf, bound_len);

        // Create temporary buffer for bound data
        StringInfoData bound_buf;
        initStringInfo(&bound_buf);
        appendBinaryStringInfo(&bound_buf, bound_data, bound_len);

        upper.val = ReceiveFunctionCall(&cache->typioproc, &bound_buf,
                                        cache->typioparam, typmod);
        pfree(bound_buf.data);
    } else {
        upper.val = (Datum) 0;
    }

    pq_getmsgend(buf); // Validate all data consumed

    // Set boundary properties from flags
    lower.infinite = (flags & RANGE_LB_INF) != 0;
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;
    upper.infinite = (flags & RANGE_UB_INF) != 0;
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    // Create and canonicalize the range
    RangeType *range = make_range(cache->typcache, &lower, &upper,
                                  flags & RANGE_EMPTY, NULL);

    PG_RETURN_RANGE_P(range);
}
```