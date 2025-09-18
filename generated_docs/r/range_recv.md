# range_recv

## Location
src/backend/utils/adt/rangetypes.c: 177 - 260

## Overview
The range_recv function is the binary receive function for PostgreSQL range types, responsible for deserializing range values from their binary wire format into internal RangeType structures.

## Definition


## Detailed Description
This function processes binary data received over the wire (typically from client connections) and reconstructs RangeType values from their serialized format. The binary format consists of a flags byte followed by the binary representations of the lower and upper bounds (when present). Each bound includes a 4-byte length header followed by the binary representation as produced by the element type's send function. The function handles flag validation, bound deserialization, and proper reconstruction of the RangeBound structures before creating the final canonicalized range.

## Parameters / Member Variables
-  (PG_GETARG_POINTER(0)): StringInfo buffer containing the binary data to deserialize
-  (PG_GETARG_OID(1)): The OID of the range type being processed
-  (PG_GETARG_INT32(2)): Type modifier for the range type

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth: Stack overflow protection for recursive calls
  - get_range_io_data: Retrieves I/O function cache data for the range type
  - pq_getmsgbyte: Reads a single byte from the message buffer
  - pq_getmsgint: Reads a 4-byte integer from the message buffer
  - pq_getmsgbytes: Reads a specified number of bytes from the message buffer
  - pq_getmsgend: Validates that all message data has been consumed
  - initStringInfo: Initializes a StringInfo buffer
  - appendBinaryStringInfo: Appends binary data to a StringInfo buffer
  - ReceiveFunctionCall: Calls the element type's receive function
  - pfree: Frees allocated memory
  - make_range: Constructs and canonicalizes the final RangeType structure
- Data structures used:
  - RangeIOData: Cache structure for I/O functions
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