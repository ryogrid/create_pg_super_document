# datumEstimateSpace

## Location
[src/backend/utils/adt/datum.c:412-458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datum.c#L412-L458)

## Overview
Computes the amount of space that datumSerialize will require to serialize a particular PostgreSQL Datum value.

## Definition
```c
Size datumEstimateSpace(Datum value, bool isnull, bool typByVal, int typLen)
```

## Detailed Description
The `datumEstimateSpace` function calculates the exact number of bytes needed to serialize a Datum value for storage or transmission. This is essential for memory allocation planning before calling `datumSerialize`. The function handles different data types and storage formats:

- Always includes space for a length indicator (sizeof(int))
- For null values, only the length indicator is needed
- For pass-by-value types, adds space for the Datum itself
- For variable-length types, checks if the value is an expanded object that needs flattening
- For expanded objects, calculates the flattened size using the Expanded Object Header (EOH)
- For other types, uses the standard `datumGetSize` function

The function is designed to work in conjunction with parallel query execution and parameter passing mechanisms where accurate space estimation is crucial for shared memory allocation.

## Parameters / Member Variables
- `value`: The Datum value to estimate space for
- `isnull`: Boolean indicating whether the value is NULL
- `typByVal`: Boolean indicating whether the type is passed by value or by reference  
- `typLen`: Length specification for the type (positive for fixed length, -1 for variable length, -2 for null-terminated strings)

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_EXPANDED (checks if value is an expanded object)
  - [DatumGetEOHP](../D/DatumGetEOHP.md) (gets Expanded Object Header Pointer)
  - [EOH_get_flat_size](../E/EOH_get_flat_size.md) (gets flattened size of expanded object)
  - [datumGetSize](datumGetSize.md) (gets size of standard datum values)
- Called from (representative examples):
  - [EstimateParamExecSpace](../E/EstimateParamExecSpace.md) (in parallel execution parameter estimation)
  - [EstimateParamListSpace](../E/EstimateParamListSpace.md) (in parameter list space estimation)

## Notes and Other Information
- The function comment notes that overflow is not possible for the space calculations, allowing simple addition
- Special handling for expanded objects is necessary because they may need to be flattened during serialization
- This function is part of the datum serialization infrastructure used in parallel query processing
- The space estimate must exactly match what `datumSerialize` will actually use
- Declared in src/include/utils/datum.h as part of the public PostgreSQL utility API

## Simplified Source

```c
Size datumEstimateSpace(Datum value, bool isnull, bool typByVal, int typLen)
{
    Size sz = sizeof(int);  // Always need space for header

    if (!isnull)
    {
        if (typByVal)
            sz += sizeof(Datum);  // Fixed size for by-value types
        else if (typLen == -1 && VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)))
        {
            // Expanded objects need flattening - get flattened size
            sz += EOH_get_flat_size(DatumGetEOHP(value));
        }
        else
            sz += datumGetSize(value, typByVal, typLen);  // Standard size calculation
    }

    return sz;
}
```