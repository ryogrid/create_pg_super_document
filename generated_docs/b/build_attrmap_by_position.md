# build_attrmap_by_position

## Location
src/backend/access/common/attmap.c: 75 - 176

## Overview
Builds an attribute map for tuple conversion by matching input and output columns by their physical positions, validating type compatibility and handling dropped columns appropriately.

## Definition
```c
AttrMap *build_attrmap_by_position(TupleDesc indesc, TupleDesc outdesc, const char *msg)
```

## Detailed Description
The `build_attrmap_by_position` function creates an attribute map that matches columns between two tuple descriptors based on their physical positions, ignoring dropped columns. It performs comprehensive validation including type and typemod compatibility checking, and reports detailed error messages if mismatches are found. The function is designed as a subroutine for `convert_tuples_by_position` but can be used standalone. 

The function follows a careful algorithm: it iterates through each non-dropped column in the output descriptor, finds the corresponding non-dropped column in the input descriptor by position, validates type compatibility, and builds the mapping. If all columns match perfectly (one-to-one), it returns NULL to indicate no conversion is needed, otherwise it returns the attribute map.

## Parameters / Member Variables
- `indesc`: Input tuple descriptor (the "returned" rowtype in error messages)
- `outdesc`: Output tuple descriptor (the "expected" rowtype in error messages) 
- `msg`: Error message context string used in error reports

## Dependencies
- Functions called/Symbols referenced:
  - `make_attrmap` (creates the basic attribute map structure)
  - `format_type_with_typemod` (formats type names for error messages)
  - `check_attrmap_match` (checks for one-to-one mapping)
  - `free_attrmap` (frees the map if no conversion needed)
  - `ereport` (error reporting)
  - `TupleDescAttr` (accesses tuple descriptor attributes)
- Called from (representative examples):
  - `convert_tuples_by_position`

## Notes and Other Information
- Dropped columns are ignored in both input and output and marked as 0 in the mapping
- Performs strict type and typemod validation - mismatches result in detailed error messages
- Returns NULL if no runtime conversion is needed (perfect one-to-one match)
- The algorithm counts non-dropped attributes separately for accurate error reporting
- Error messages refer to indesc as "returned" and outdesc as "expected" rowtype
- Used primarily in tuple conversion scenarios where column order is preserved
- Located in `src/backend/access/common/attmap.c:75-176`