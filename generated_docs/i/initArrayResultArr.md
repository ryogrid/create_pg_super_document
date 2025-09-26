# initArrayResultArr

## Location
src/backend/utils/adt/arrayfuncs.c: 5492 - 5537

## Overview
Initializes an empty ArrayBuildStateArr structure for building arrays from input arrays, creating an output array with N+1 dimensions.

## Definition

```c
ArrayBuildStateArr *
initArrayResultArr(Oid array_type, Oid element_type, MemoryContext rcontext,
				   bool subcontext)
```
## Detailed Description
This function is part of a specialized API for building arrays from arrays (as opposed to building arrays from individual elements). It initializes an ArrayBuildStateArr structure that will be used to accumulate input arrays and eventually produce an output array with one additional dimension. All input arrays must have identical dimensionality and element type.

The function performs element type lookup if not provided and optionally creates a separate memory context for the working state. All fields in the returned structure are initialized to zero, providing a clean starting state for array accumulation.

## Parameters / Member Variables
- : OID of the array type (must be a valid varlena array type)
- : OID of the array's element type (looked up from array_type if InvalidOid)
- : Memory context where working state should be kept
- : Flag determining whether to create a separate memory context for array building

## Dependencies
- Functions called/Symbols referenced:
  - get_element_type
  - AllocSetContextCreate
  - MemoryContextAllocZero
  - format_type_be (in error reporting)
  - OidIsValid
- Called from (representative examples):
  - array_agg_array_transfn
  - array_agg_array_combine
  - array_agg_array_deserialize
  - accumArrayResultArr

## Notes and Other Information
- This function is part of a three-function API: initArrayResultArr/accumArrayResultArr/makeArrayResultArr
- Unlike initArrayResult which works with individual elements, this API works with entire arrays as inputs
- The resulting array will have N+1 dimensions where N is the dimensionality of input arrays
- All input arrays must have identical dimensionality and element type
- If subcontext=true, creates a memory context named "accumArrayResultArr" for managing temporary data
- Throws an error if the provided array_type is not actually an array type