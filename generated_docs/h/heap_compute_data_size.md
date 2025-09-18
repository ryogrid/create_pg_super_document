# heap_compute_data_size

## Location
src/backend/access/common/heaptuple.c: 215 - 270

## Overview
The `heap_compute_data_size` function calculates the total size of the data area needed to construct a heap tuple from an array of Datum values, considering alignment requirements and various storage optimizations.

## Definition
```c
Size heap_compute_data_size(TupleDesc tupleDesc, const Datum *values, const bool *isnull)
```

## Detailed Description
This function iterates through all attributes in a tuple descriptor and calculates the exact size needed for the data portion of a heap tuple. It handles three main cases: (1) packable variable-length attributes that can use short headers for space optimization, (2) externally expanded variable-length attributes that need to be flattened, and (3) standard attributes with normal alignment and size requirements. The function accounts for proper alignment padding between attributes and applies various storage optimizations like TOAST compression where applicable. It skips null attributes in the calculation.

## Parameters / Member Variables
- `tupleDesc`: Tuple descriptor containing metadata about each attribute (type, length, alignment, etc.)
- `values`: Array of Datum values to be stored in the tuple
- `isnull`: Array of boolean flags indicating which values are null

## Dependencies
- Functions called/Symbols referenced:
  - `TupleDescAttr`: Macro to access attribute descriptor from tuple descriptor
  - `ATT_IS_PACKABLE`: Macro to check if attribute can use packed storage
  - `VARATT_CAN_MAKE_SHORT`: Macro to check if variable-length attribute can use short header
  - `VARATT_CONVERTED_SHORT_SIZE`: Macro to get size when converted to short header format
  - `VARATT_IS_EXTERNAL_EXPANDED`: Macro to check if attribute is externally expanded
  - `att_align_nominal`: Function to align data according to attribute alignment requirements
  - `att_align_datum`: Function to align data for a specific Datum value
  - `att_addlength_datum`: Function to add the length of a Datum to the running total
  - `[DatumGetPointer](../D/DatumGetPointer.md)`: Macro to extract pointer from Datum
  - `DatumGetEOHP`: Macro to get expanded object header pointer
  - `EOH_get_flat_size`: Function to get size needed to flatten expanded object
- Called from (representative examples):
  - `[heap_form_tuple](heap_form_tuple.md)`: Uses this to determine tuple size before allocation
  - `[heap_form_minimal_tuple](heap_form_minimal_tuple.md)`: Uses this for minimal tuple size calculation
  - `[heap_toast_insert_or_update](heap_toast_insert_or_update.md)`: Uses this to determine if TOAST processing is needed
  - `[brin_form_tuple](../b/brin_form_tuple.md)`: Uses this for BRIN index tuple size calculation
  - `[index_form_tuple_context](../i/index_form_tuple_context.md)`: Uses this for index tuple size calculation

## Notes and Other Information
- Returns the total size in bytes needed for the data area of the tuple
- Does not include the size of the tuple header itself, only the data portion
- Handles storage optimizations like short variable-length headers and expanded object flattening
- Critical for memory allocation before tuple construction to avoid buffer overruns
- Used extensively in tuple formation throughout PostgreSQL access methods
- Accounts for alignment padding required between attributes based on their data types
- Part of PostgreSQL core tuple manipulation infrastructure