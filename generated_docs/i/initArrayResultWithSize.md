# initArrayResultWithSize

## Location
[src/backend/utils/adt/arrayfuncs.c:5298-5337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5298-L5337)

## Overview
Initializes an ArrayBuildState structure with a specified initial array size, providing fine-grained control over memory allocation for array building operations.

## Definition

```c
ArrayBuildState *
initArrayResultWithSize(Oid element_type, MemoryContext rcontext,
						bool subcontext, int initsize)
```
## Detailed Description
This function creates and initializes an ArrayBuildState structure for building arrays incrementally, allowing the caller to specify the initial size of the allocated arrays. It performs the core initialization work for array building, setting up memory contexts, allocating initial storage for element values and null flags, and retrieving type information.

The function supports flexible memory management through the subcontext parameter:
- When subcontext=true: creates a separate AllocSet context named "accumArrayResult"
- When subcontext=false: uses the provided rcontext directly

The function allocates arrays for both element values (Datum) and null flags (bool) based on the specified initial size, and retrieves type-specific information including length, pass-by-value status, and alignment requirements.

## Parameters / Member Variables
- : OID of the array element type (must be a valid array element type)
- : Memory context where working state should be kept  
- : Flag determining whether to use a separate memory context for this build state
- : Initial size for the allocated value and null arrays

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (creates new memory context when subcontext=true)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocates memory for state structure and arrays)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md) (retrieves type information for the element type)
- Called from (representative examples):
  - [initArrayResult](initArrayResult.md) (wrapper function with default sizes)
  - [array_agg_combine](../a/array_agg_combine.md) (combining array aggregation states)
  - [array_agg_deserialize](../a/array_agg_deserialize.md) (deserializing array aggregation states)

## Notes and Other Information
- This is the core initialization function that does the actual work for array building setup
- The ArrayBuildState structure contains all necessary information for incrementally building arrays
- Memory allocation is performed in the specified context (either rcontext or a new subcontext)
- Type information is cached in the state structure for efficient element handling
- The initial size can be tuned based on expected number of elements to optimize memory usage
- Arrays will be automatically resized if more elements are added than the initial size

## Simplified Source

```c
ArrayBuildState *initArrayResultWithSize(Oid element_type, MemoryContext rcontext,
                                         bool subcontext, int initsize) {
    ArrayBuildState *astate;
    MemoryContext arr_context = rcontext;

    // Create temporary context if requested
    if (subcontext)
        arr_context = AllocSetContextCreate(rcontext, "accumArrayResult",
                                           ALLOCSET_DEFAULT_SIZES);

    // Allocate and initialize the build state structure
    astate = (ArrayBuildState *) MemoryContextAlloc(arr_context, sizeof(ArrayBuildState));
    astate->mcontext = arr_context;
    astate->private_cxt = subcontext;
    astate->alen = initsize;

    // Allocate initial arrays for values and null flags
    astate->dvalues = (Datum *) MemoryContextAlloc(arr_context, astate->alen * sizeof(Datum));
    astate->dnulls = (bool *) MemoryContextAlloc(arr_context, astate->alen * sizeof(bool));

    // Initialize counters and type information
    astate->nelems = 0;
    astate->element_type = element_type;
    get_typlenbyvalalign(element_type, &astate->typlen, &astate->typbyval, &astate->typalign);

    return astate;
}
```