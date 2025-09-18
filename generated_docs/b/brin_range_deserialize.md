# brin_range_deserialize

## Location
src/backend/access/brin/brin_minmax_multi.c: 721 - 857

## Overview
Deserializes a compact varlena SerializedRanges value back into the in-memory Ranges representation for BRIN index operations.

## Definition


## Detailed Description
This function performs the reverse operation of brin_range_serialize, taking a SerializedRanges structure and reconstructing the in-memory Ranges representation. The deserialization process handles different data types appropriately:

- **By-value types**: Uses fetch_att to properly reconstruct Datum values with correct alignment
- **Fixed-length by-reference types**: Copies data to a newly allocated buffer with proper alignment
- **Variable-length types (varlena)**: Copies the entire varlena structure to properly aligned memory
- **C-string types**: Copies strings including null terminators to aligned memory

The function allocates memory efficiently by calculating the total space needed for all by-reference data types in advance and allocating it as a single chunk. This reduces memory fragmentation and allocation overhead. The deserialized values array is properly reconstructed with correct data type handling and alignment requirements.

## Parameters / Member Variables
- : Maximum number of values the resulting Ranges structure should support
- : The SerializedRanges structure to deserialize

## Dependencies
- Functions called/Symbols referenced:
  - minmax_multi_init
  - get_typbyval
  - get_typlen
  - VARSIZE_ANY
  - fetch_att
  - PointerGetDatum
  - MAXALIGN
- Called from (representative examples):
  - brin_minmax_multi_add_value
  - brin_minmax_multi_consistent
  - brin_minmax_multi_union
  - brin_minmax_multi_summary_out

## Notes and Other Information
- The function performs sanity checks using Assert() to validate the serialized data structure
- Memory allocation is optimized by calculating total space needed and allocating once
- Proper alignment is maintained for all data types using MAXALIGN
- The nsorted field is initialized to nvalues, indicating all values are considered sorted after deserialization
- The function ensures exact consumption of the serialized input data
- Comment mentions by-value types don't need copying but the implementation still handles them for consistency