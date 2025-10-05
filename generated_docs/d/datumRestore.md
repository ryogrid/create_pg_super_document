# datumRestore

## Location
[src/backend/utils/adt/datum.c:521-556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datum.c#L521-L556)

## Overview
Restores a possibly-NULL PostgreSQL Datum that was previously serialized by datumSerialize, deserializing it from a binary format back into a usable Datum value.

## Definition
```c
Datum datumRestore(char **start_address, bool *isnull)
```

## Detailed Description
The `datumRestore` function is the counterpart to `datumSerialize`, reconstructing a Datum value from its serialized binary representation. The function reads the serialization format created by `datumSerialize` and recreates the original Datum value with appropriate memory management.

The deserialization process follows these steps:
1. Reads the 4-byte header word to determine the data type and size
2. Based on the header value:
   - -2: Sets isnull to true and returns 0 for NULL values
   - -1: Reads sizeof(Datum) bytes for pass-by-value types
   - Positive value: Allocates memory and copies the specified number of bytes for pass-by-reference data

For pass-by-reference data, the function allocates new memory using `palloc` and copies the serialized data into it. This ensures that the restored Datum has its own memory space independent of the serialization buffer.

The function updates the `start_address` pointer to point immediately after the consumed data, enabling sequential deserialization of multiple values.

## Parameters / Member Variables  
- `start_address`: Pointer to the serialized data location, updated to point after consumed data
- `isnull`: Output parameter set to indicate whether the restored value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts pointer to Datum)
  - Assert (assertion macro for debugging)
- Called from (representative examples):
  - [RestoreParamExecParams](../R/RestoreParamExecParams.md) (in parallel execution parameter restoration)
  - [RestoreParamList](../R/RestoreParamList.md) (in parameter list restoration)

## Notes and Other Information
- The function assumes the serialized data was created by `datumSerialize` and follows the same format
- Memory allocation is handled automatically for pass-by-reference types
- The caller is responsible for eventually freeing any allocated memory for pass-by-reference values
- TOAST pointers in the serialized data are preserved and remain valid within the same database server
- Used primarily in parallel query processing for inter-process data transfer
- The function does not validate the serialized data format - corrupted input may cause undefined behavior
- Declared in src/include/utils/datum.h as part of the public PostgreSQL utility API
- Works in conjunction with `datumEstimateSpace` and `datumSerialize` as part of the complete serialization framework

## Simplified Source

```c
Datum datumRestore(char **start_address, bool *isnull) {
    int header;

    // Read header to determine data type
    memcpy(&header, *start_address, sizeof(int));
    *start_address += sizeof(int);

    // Handle NULL datum
    if (header == -2) {
        *isnull = true;
        return (Datum) 0;
    }

    *isnull = false;

    // Handle pass-by-value datum
    if (header == -1) {
        Datum val;
        memcpy(&val, *start_address, sizeof(Datum));
        *start_address += sizeof(Datum);
        return val;
    }

    // Handle pass-by-reference datum
    // Allocate memory and copy data
    void *data = palloc(header);
    memcpy(data, *start_address, header);
    *start_address += header;

    return PointerGetDatum(data);
}
```