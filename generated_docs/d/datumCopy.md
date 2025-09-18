# datumCopy

## Location
src/backend/utils/adt/datum.c: 132 - 193

## Overview
Creates a deep copy of a non-NULL datum, allocating new memory with palloc() for pass-by-reference types and handling expanded objects by flattening them into regular storage.

## Definition


## Detailed Description
The  function creates a complete copy of a datum, handling the complexities of PostgreSQL's various datum storage mechanisms. The function's behavior depends on the type characteristics:

1. **Pass-by-value types**: Simply returns the value unchanged since it's already copied
2. **Variable-length (varlena) types**: 
   - For expanded objects, flattens them into a contiguous memory block using the expanded object interface
   - For regular varlena data, performs a byte-for-byte copy including the varlena header
3. **Fixed-length pass-by-reference types**: Allocates new memory and copies the data

The function is particularly important when copying datums out of transient memory contexts that are about to be destroyed, ensuring the copied datum survives in the caller's memory context. For expanded objects, flattening is necessary because the expanded object may reside in a child context that will be destroyed.

## Parameters / Member Variables
- : The datum value to be copied
- : Boolean indicating whether the type is passed by value (true) or by reference (false)
- : The declared type length (-1 for varlena, positive for fixed-length)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - VARATT_IS_EXTERNAL_EXPANDED
  - DatumGetEOHP
  - EOH_get_flat_size
  - EOH_flatten_into
  - VARSIZE_ANY
  - [datumGetSize](datumGetSize.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - [union_tuples](../u/union_tuples.md) (BRIN)
  - [brin_inclusion_add_value](../b/brin_inclusion_add_value.md)
  - [brin_minmax_add_value](../b/brin_minmax_add_value.md)
  - [ExecAggInitGroup](../E/ExecAggInitGroup.md)
  - [advance_transition_function](../a/advance_transition_function.md)
  - [_copyConst](../c/_copyConst.md)
  - [accumArrayResult](../a/accumArrayResult.md)
  - [datumTransfer](datumTransfer.md)
  - [tuplesort_putdatum](../t/tuplesort_putdatum.md)

## Notes and Other Information
- The function assumes the input datum is non-NULL; NULL datums should be handled by the caller
- For expanded objects, the function flattens them to ensure the result is a single pfree-able chunk
- All allocated memory uses palloc(), making it subject to PostgreSQL's memory context management
- Pass-by-value types require no memory allocation since the value is already copied by parameter passing
- The function is crucial for datum lifecycle management in complex operations like aggregation and sorting
- Callers can assume the returned datum is independent of the original datum's memory