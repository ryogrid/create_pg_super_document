# ArrayBuildState

## Location
[src/include/utils/array.h:187-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/array.h#L187-L199)

## Overview
ArrayBuildState is a working state structure used by accumArrayResult() and related functions to efficiently build arrays by accumulating scalar elements one at a time.

## Definition
```c
typedef struct ArrayBuildState
{
    MemoryContext mcontext;     /* where all the temp stuff is kept */
    Datum      *dvalues;        /* array of accumulated Datums */
    bool       *dnulls;         /* array of is-null flags for Datums */
    int         alen;           /* allocated length of above arrays */
    int         nelems;         /* number of valid entries in above arrays */
    Oid         element_type;   /* data type of the Datums */
    int16       typlen;         /* needed info about datatype */
    bool        typbyval;
    char        typalign;
    bool        private_cxt;    /* use private memory context */
} ArrayBuildState;
```

## Detailed Description
ArrayBuildState provides an efficient mechanism for building arrays incrementally by accumulating individual elements. It manages memory allocation, handles both pass-by-value and pass-by-reference data types, and tracks null values. The structure maintains arrays that grow dynamically as elements are added via accumArrayResult(). When the final array is needed, makeArrayResult() converts the accumulated elements into a proper ArrayType structure. The state tracks essential datatype information to ensure proper handling of elements during accumulation and final array construction.

## Parameters / Member Variables
- `mcontext`: Memory context where temporary data and accumulated elements are stored
- `dvalues`: Dynamically allocated array holding the accumulated Datum values
- `dnulls`: Parallel array of boolean flags indicating which elements are NULL
- `alen`: Current allocated capacity of the dvalues and dnulls arrays
- `nelems`: Number of valid elements currently stored in the arrays
- `element_type`: OID of the data type for array elements
- `typlen`: Length of the element data type (-1 for variable-length types)
- `typbyval`: Whether elements are passed by value or by reference
- `typalign`: Alignment requirement for the element data type
- `private_cxt`: Flag indicating whether a private memory context is being used

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContext](../M/MemoryContext.md) for memory management
  - Datum type for element storage
  - Oid type for element type identification
- Called from (representative examples):
  - [accumArrayResult](../a/accumArrayResult.md)() - [main](../m/main.md) function for adding elements
  - [initArrayResult](../i/initArrayResult.md)() - initializes the state
  - [makeArrayResult](../m/makeArrayResult.md)() - creates final ArrayType from accumulated state
  - [array_agg_transfn](../a/array_agg_transfn.md)() - array aggregation function
  - Various array-building functions throughout the codebase

## Notes and Other Information
- Input elements must be scalars (legal array elements), not arrays themselves
- The structure automatically grows the storage arrays when capacity is exceeded
- Pass-by-reference data is copied into the memory context to ensure data persistence
- [Variable](../V/Variable.md)-length (varlena) data is detoasted and copied to avoid later modifications
- Used extensively in aggregate functions like array_agg() and various array manipulation routines
- The private_cxt flag allows for using a private memory context for better memory management control