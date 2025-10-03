# initArrayResultArr

## Location
[src/backend/utils/adt/arrayfuncs.c:5492-5537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5492-L5537)

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
- `array_type`: OID of the array type (must be a valid varlena array type)
- `element_type`: OID of the array's element type (looked up from array_type if InvalidOid)
- `rcontext`: Memory context where working state should be kept
- `subcontext`: Flag determining whether to create a separate memory context for array building
## Dependencies
- Functions called/Symbols referenced:
  - [get_element_type](../g/get_element_type.md)
  - AllocSetContextCreate
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [format_type_be](../f/format_type_be.md) (in error reporting)
  - OidIsValid
- Called from (representative examples):
  - [array_agg_array_transfn](../a/array_agg_array_transfn.md)
  - [array_agg_array_combine](../a/array_agg_array_combine.md)
  - [array_agg_array_deserialize](../a/array_agg_array_deserialize.md)
  - [accumArrayResultArr](../a/accumArrayResultArr.md)

## Notes and Other Information
- This function is part of a three-function API: initArrayResultArr/accumArrayResultArr/makeArrayResultArr
- Unlike initArrayResult which works with individual elements, this API works with entire arrays as inputs
- The resulting array will have N+1 dimensions where N is the dimensionality of input arrays
- All input arrays must have identical dimensionality and element type
- If subcontext=true, creates a memory context named "accumArrayResultArr" for managing temporary data
- Throws an error if the provided array_type is not actually an array type

## Simplified Source

```c
ArrayBuildStateArr *initArrayResultArr(Oid array_type, Oid element_type,
                                      MemoryContext rcontext, bool subcontext) {
    MemoryContext arr_context = rcontext;  // Default to parent context

    // Lookup element type if not provided
    if (!OidIsValid(element_type)) {
        element_type = get_element_type(array_type);

        if (!OidIsValid(element_type))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                          errmsg("data type %s is not an array type",
                                format_type_be(array_type))));
    }

    // Create separate memory context if requested
    if (subcontext)
        arr_context = AllocSetContextCreate(rcontext, "accumArrayResultArr",
                                          ALLOCSET_DEFAULT_SIZES);

    // Allocate and zero-initialize the state structure
    ArrayBuildStateArr *astate = (ArrayBuildStateArr *)
        MemoryContextAllocZero(arr_context, sizeof(ArrayBuildStateArr));

    astate->mcontext = arr_context;
    astate->private_cxt = subcontext;

    // Save datatype information
    astate->array_type = array_type;
    astate->element_type = element_type;

    return astate;
}
```