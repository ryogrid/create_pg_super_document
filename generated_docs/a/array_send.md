# array_send

## Location
src/backend/utils/adt/arrayfuncs.c: 1548 - 1651

## Overview
Converts PostgreSQL arrays from internal ArrayType representation to external binary format for network transmission or storage serialization.

## Definition


## Detailed Description
array_send is the binary send function for PostgreSQL arrays, responsible for serializing internal ArrayType structures into binary format suitable for network transmission or storage. The function uses PostgreSQL's standard binary protocol format, which includes array metadata (dimensions, bounds, null flags) followed by individual element data. It employs a caching mechanism (ArrayMetaState) to optimize performance across multiple function calls with the same element type.

The serialization process involves writing array header information (dimensions, null flag, element type, dimension sizes and bounds), followed by individual elements using element-specific send procedures. NULL elements are represented with a special -1 length marker, while non-null elements include their data length followed by the actual binary data. The function ensures proper memory management by freeing temporary bytea objects created during element serialization.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro - the array is retrieved via PG_GETARG_ANY_ARRAY_P(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ANY_ARRAY_P
  - AARR_ELEMTYPE
  - AARR_NDIM
  - AARR_DIMS
  - AARR_LBOUND
  - AARR_HASNULL
  - ArrayGetNItems
  - [get_type_io_data](../g/get_type_io_data.md)
  - IOFunc_send
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - array_iter_setup
  - array_iter_next
  - [SendFunctionCall](../S/SendFunctionCall.md)
  - pq_sendbytes
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - VARSIZE
  - VARDATA
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - [int2vectorsend](../i/int2vectorsend.md)
  - [oidvectorsend](../o/oidvectorsend.md)
  - [anyarray_send](anyarray_send.md)
  - [anycompatiblearray_send](anycompatiblearray_send.md)
  - [CATALOG](../C/CATALOG.md) (pg_type.h)

## Notes and Other Information
The function uses ArrayMetaState caching to store element type information and avoid repeated type system lookups, improving performance for repeated operations. The binary protocol format is PostgreSQL-specific and includes comprehensive metadata to enable proper deserialization. NULL elements are efficiently represented using -1 length markers without additional data. The function ensures that element types have valid binary send procedures and reports appropriate errors for types lacking binary output support. Memory management is handled carefully with proper cleanup of temporary bytea objects to prevent memory leaks during serialization.