# range_send

## Location
src/backend/utils/adt/rangetypes.c: 261 - 316

## Overview
The range_send function is the binary send function for PostgreSQL range types, responsible for serializing internal RangeType structures into binary wire format for transmission.

## Definition


## Detailed Description
This function converts a RangeType value from its internal representation to a binary format suitable for transmission over the wire (typically to client connections). The binary output format consists of a flags byte followed by the binary representations of the lower and upper bounds (when present). Each bound is prefixed with a 4-byte length header followed by the binary data as produced by the element type's send function. The function handles deserialization of the input range, proper serialization of boundaries, and construction of the complete binary message.

## Parameters / Member Variables
-  (PG_GETARG_RANGE_P(0)): The input RangeType structure to be serialized to binary format

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth: Stack overflow protection for recursive calls
  - makeStringInfo: Creates a new StringInfo buffer for output
  - get_range_io_data: Retrieves I/O function cache data for the range type
  - RangeTypeGetOid: Extracts the OID from the range type
  - range_deserialize: Extracts boundary information from the range
  - range_get_flags: Retrieves the range flags
  - pq_begintypsend: Initializes the output buffer for type sending
  - pq_sendbyte: Writes a single byte to the output buffer
  - pq_sendint32: Writes a 4-byte integer to the output buffer
  - pq_sendbytes: Writes binary data to the output buffer
  - pq_endtypsend: Finalizes the output buffer and returns the result
  - SendFunctionCall: Calls the element type's send function for boundaries
  - PointerGetDatum: Converts pointer to Datum for function calls
  - VARSIZE/VARDATA: Macros for extracting size and data from varlena types
- Data structures used:
  - RangeIOData: Cache structure for I/O functions
  - RangeBound: Structure representing range boundaries
  - StringInfo: Buffer structure for building binary output
  - IOFunc_send: Enum value for send function type
- Macros used:
  - PG_GETARG_RANGE_P: Macro to extract range argument
  - RANGE_HAS_LBOUND/RANGE_HAS_UBOUND: Check for boundary existence
  - VARHDRSZ: Size of variable-length header
  - PG_RETURN_BYTEA_P: Return macro for binary data

## Notes and Other Information
- The binary format mirrors that expected by range_recv: flags byte followed by bounds with length headers
- Each bound is serialized using the element type's send function, then wrapped with a length prefix
- The function uses PostgreSQL's standard pq_* functions for constructing binary protocol messages
- Only existing bounds (not infinite ones) are serialized in the output
- Memory management is handled automatically by the StringInfo and pq_endtypsend infrastructure
- The function includes stack depth checking for recursive serialization of nested range types
- VARSIZE and VARDATA macros are used to extract binary data from the varlena results of SendFunctionCall