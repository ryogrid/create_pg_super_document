# PGTYPESnumeric_copy

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 1388 - 1410

## Overview
Creates a deep copy of a PostgreSQL numeric value, duplicating all numeric properties and digit data from source to destination.

## Definition
```c
int PGTYPESnumeric_copy(numeric *src, numeric *dst)
```

## Detailed Description
This function performs a complete deep copy of a numeric value from source to destination. It first validates the destination pointer, then initializes the destination by zeroing it out. The function copies all numeric metadata (weight, rscale, dscale, sign) and allocates appropriate memory for the destination's digit array. Finally, it copies all digit values from the source to the destination, creating an independent copy of the numeric value.

The copying process ensures that:
1. The destination is properly initialized and zeroed
2. All numeric metadata is transferred accurately
3. Sufficient memory is allocated for the digit array
4. All digit values are copied individually
5. The resulting copy is completely independent of the source

## Parameters / Member Variables
- `src`: Pointer to the source numeric value to be copied
- `dst`: Pointer to the destination numeric structure that will receive the copy

## Dependencies
- Functions called/Symbols referenced:
  - zero_var (initializes numeric variable to zero)
  - alloc_var (allocates memory for numeric digits)
  - numeric (type definition)
- Called from (representative examples):
  - ecpg_get_data (ECPG data retrieval)
  - ecpg_store_input (ECPG input storage)
  - PGTYPESnumeric_to_asc (numeric to string conversion)
  - PGTYPESnumeric_from_double (double to numeric conversion)
  - numericvar_to_double (numeric to double conversion)

## Notes and Other Information
- Returns 0 on success, -1 on failure (typically due to NULL destination pointer or memory allocation failure)
- Performs null pointer validation on the destination before proceeding
- Creates a completely independent copy - modifications to the copy will not affect the original
- Part of the ECPG pgtypes library for PostgreSQL embedded C programming
- Essential for numeric value management in ECPG applications where numeric values need to be duplicated or preserved