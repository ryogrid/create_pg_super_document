# datumGetSize

## Location
[src/backend/utils/adt/datum.c:65-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datum.c#L65-L131)

## Overview
Determines the actual memory size required to store a PostgreSQL datum, handling the different storage methods (by-value, fixed-length by-reference, variable-length, and C-string types).

## Definition

```c
struct varlena *s = (struct varlena *) DatumGetPointer(value);
```
## Detailed Description
The  function calculates the "real" size of a datum given its value and type characteristics. This is an out-of-line version of the  macro in , with additional error checking. The function handles four different datum storage scenarios:

1. **Pass-by-value types**: Returns the fixed type length directly
2. **Fixed-length pass-by-reference types**: Returns the declared type length
3. **Variable-length (varlena) types**: Uses  to determine the actual size including header
4. **C-string types**: Calculates string length plus null terminator

For TOAST pointer datums, this returns the size of the pointer datum itself, not the detoasted data.

## Parameters / Member Variables
- : The datum value whose size is to be determined
- : Boolean indicating whether the type is passed by value (true) or by reference (false)
- : The declared type length (-1 for varlena, -2 for cstring, positive for fixed-length)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - PointerIsValid
  - VARSIZE_ANY
  - strlen
  - ereport/elog (for error handling)
- Called from (representative examples):
  - [outDatum](../o/outDatum.md)
  - [datumCopy](datumCopy.md)
  - [datumIsEqual](datumIsEqual.md)
  - [datumEstimateSpace](datumEstimateSpace.md)
  - [datumSerialize](datumSerialize.md)
  - [writetup_datum](../w/writetup_datum.md)

## Notes and Other Information
- The function includes comprehensive error checking for invalid datum pointers
- Pass-by-value types are asserted to have positive length not exceeding sizeof(Datum)
- For varlena types (typLen == -1), the function uses  which handles both regular and short varlena headers
- For C-string types (typLen == -2), the size includes the null terminator
- Invalid typLen values result in an ERROR being thrown
- This function is essential for memory management operations involving datums