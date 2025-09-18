# make_range

## Location
src/backend/utils/adt/rangetypes.c: 1952 - 2015

## Overview
Creates and returns a properly serialized and canonicalized RangeType from the provided bounds, handling both empty and non-empty ranges.

## Definition


## Detailed Description
The  function is a high-level constructor for RangeType objects that performs both serialization and canonicalization (when applicable). It serves as the primary entry point for most callers who need to create range objects. The function first serializes the range using , then applies the canonical function if one is defined for the range type and the range is not empty. This ensures that equivalent ranges have identical internal representations, which is crucial for proper comparison and indexing operations.

## Parameters / Member Variables
- : Type cache entry containing metadata about the range type, including canonicalization function information
- : Pointer to the lower bound of the range
- : Pointer to the upper bound of the range  
- : Boolean flag indicating whether to create an empty range
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - range_serialize
  - SOFT_ERROR_OCCURRED
  - RangeIsEmpty
  - LOCAL_FCINFO
  - InitFunctionCallInfoData
  - RangeTypePGetDatum
  - FunctionCallInvoke
  - DatumGetRangeTypeP
- Called from (representative examples):
  - range_in
  - range_recv
  - range_constructor2
  - range_constructor3
  - range_union_internal
  - range_intersect_internal
  - make_empty_range

## Notes and Other Information
- This function handles soft errors through the escontext parameter, returning NULL when errors occur
- Canonicalization is only applied to non-empty ranges when a canonical function is available
- The function is the recommended way to create RangeType objects as it ensures proper serialization and canonicalization
- Empty ranges bypass canonicalization since they have a standardized representation regardless of bounds