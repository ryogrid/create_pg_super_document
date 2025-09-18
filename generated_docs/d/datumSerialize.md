# datumSerialize

## Location
src/backend/utils/adt/datum.c: 459 - 520

## Overview
Serializes a possibly-NULL PostgreSQL Datum into caller-provided storage for inter-process communication within the same database server.

## Definition
```c
void datumSerialize(Datum value, bool isnull, bool typByVal, int typLen, char **start_address)
```

## Detailed Description
The `datumSerialize` function converts a Datum value into a portable binary format that can be transferred between processes within the same PostgreSQL database server. The function handles various data types and storage formats while maintaining proper memory layout for deserialization.

The serialization format consists of:
1. A 4-byte header word indicating the data type and size:
   - -2 for NULL values
   - -1 for pass-by-value types  
   - Positive value for the byte length of pass-by-reference data
2. Payload data (if not NULL):
   - For pass-by-value: sizeof(Datum) bytes containing the value
   - For pass-by-reference: the actual data bytes

Special handling is provided for "expanded" objects, which are flattened to create a self-contained representation. Other TOAST pointers are transferred as-is since the target process can access the same TOAST storage.

The function updates the `start_address` pointer to point immediately after the written data, enabling sequential serialization of multiple values.

## Parameters / Member Variables
- `value`: The Datum value to serialize
- `isnull`: Boolean indicating whether the value is NULL
- `typByVal`: Boolean indicating whether the type is passed by value or by reference
- `typLen`: Length specification for the type (positive for fixed length, -1 for variable length, -2 for null-terminated strings)
- `start_address`: Pointer to the storage location, updated to point after written data

## Dependencies
- Functions called/Symbols referenced:
  - ExpandedObjectHeader (type for expanded object metadata)
  - VARATT_IS_EXTERNAL_EXPANDED (checks if value is an expanded object)
  - DatumGetEOHP (gets Expanded Object Header Pointer)
  - EOH_get_flat_size (gets flattened size of expanded object)
  - datumGetSize (gets size of standard datum values)
  - EOH_flatten_into (flattens expanded object into buffer)
  - palloc/pfree (memory allocation/deallocation)
- Called from (representative examples):
  - SerializeParamExecParams (in parallel execution parameter serialization)
  - SerializeParamList (in parameter list serialization)

## Notes and Other Information
- The caller must ensure sufficient storage space using `datumEstimateSpace()` before calling this function
- Expanded objects require special handling with temporary allocation due to alignment requirements of `EOH_flatten_into`
- TOAST pointers are preserved as-is for intra-server communication efficiency
- The function is designed for parallel query processing where data needs to be shared between worker processes
- Memory layout is platform-specific and not suitable for cross-platform serialization
- Declared in src/include/utils/datum.h as part of the public PostgreSQL utility API