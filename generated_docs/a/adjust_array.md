# adjust_array

## Location
src/interfaces/ecpg/preproc/variable.c: 515 - 628

## Overview
Adjusts array dimension and length parameters for ECPG variable types based on type specifications, pointer levels, and various constraints to ensure proper array handling in embedded SQL contexts.

## Definition
```c
void adjust_array(enum ECPGttype type_enum, char **dimension, char **length, 
                 char *type_dimension, char *type_index, int pointer_len, bool type_definition)
```

## Detailed Description
The `adjust_array` function is a complex array parameter adjustment routine in the ECPG preprocessor that handles the intricate logic of setting up array dimensions and lengths for different data types. It processes various combinations of array indices, type dimensions, and pointer levels while enforcing PostgreSQL's constraints on multidimensional arrays and multilevel pointers.

The function performs several key operations:
1. Validates and restricts multidimensional arrays (not supported)
2. Limits pointer levels to maximum of 2
3. Handles special cases for different data types (struct/union, varchar/bytea, char/string)
4. Adjusts dimension and length parameters based on pointer levels and type specifications
5. Ensures proper array bounds handling for various scenarios

## Parameters / Member Variables
- `type_enum`: The ECPG type enumeration value indicating the base data type
- `dimension`: Pointer to dimension string (modified by function)
- `length`: Pointer to length string (modified by function)  
- `type_dimension`: Type-specified dimension string
- `type_index`: Type-specified index string
- `pointer_len`: Number of pointer indirection levels
- `type_definition`: Boolean indicating if this is a type definition context

## Dependencies
- Functions called/Symbols referenced:
  - ECPGttype (enum type)
  - atoi (string to integer conversion)
  - mmfatal (fatal error reporting)
  - ngettext (internationalization function)
  - mm_strdup (memory-managed string duplication)
  - strcmp (string comparison)
  - PARSE_ERROR (error constant)
  - ECPGt_struct, ECPGt_union, ECPGt_varchar, ECPGt_bytea (enum values)
  - ECPGt_char, ECPGt_unsigned_char, ECPGt_string (enum values)

- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- This function is part of the ECPG (Embedded SQL in C) preprocessor infrastructure
- Located in src/interfaces/ecpg/preproc/variable.c:515-628
- Implements PostgreSQL's restrictions on multidimensional arrays
- Handles complex pointer and array interaction rules
- Critical for proper C array to SQL array mapping in embedded contexts
- Contains specialized logic for string types (char, varchar) vs other data types
- Uses memory-managed string operations (mm_strdup) for safe memory handling