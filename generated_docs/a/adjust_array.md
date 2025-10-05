# adjust_array

## Location
[src/interfaces/ecpg/preproc/variable.c:515-628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L515-L628)

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
  - [mm_strdup](../m/mm_strdup.md) (memory-managed string duplication)
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

## Simplified Source

```c
void
adjust_array(enum ECPGttype type_enum, char **dimension, char **length,
             char *type_dimension, char *type_index, int pointer_len, bool type_definition)
{
    // Handle type index parameter
    if (atoi(type_index) >= 0) {
        if (atoi(*length) >= 0)
            mmfatal(PARSE_ERROR, "multidimensional arrays are not supported");
        *length = type_index;
    }

    // Handle type dimension parameter
    if (atoi(type_dimension) >= 0) {
        if (atoi(*dimension) >= 0 && atoi(*length) >= 0)
            mmfatal(PARSE_ERROR, "multidimensional arrays are not supported");
        if (atoi(*dimension) >= 0)
            *length = *dimension;
        *dimension = type_dimension;
    }

    // Validate pointer levels
    if (pointer_len > 2)
        mmfatal(PARSE_ERROR, "multilevel pointers (more than 2 levels) are not supported");

    if (pointer_len > 1 && type_enum != ECPGt_char && type_enum != ECPGt_unsigned_char && type_enum != ECPGt_string)
        mmfatal(PARSE_ERROR, "pointer to pointer is not supported for this data type");

    // Type-specific adjustments
    switch (type_enum) {
        case ECPGt_struct:
        case ECPGt_union:
            if (pointer_len) {
                *length = *dimension;
                *dimension = mm_strdup("0");
            }
            if (atoi(*length) >= 0)
                mmfatal(PARSE_ERROR, "multidimensional arrays for structures are not supported");
            break;

        case ECPGt_varchar:
        case ECPGt_bytea:
            if (pointer_len)
                *dimension = mm_strdup("0");
            if (atoi(*length) < 0) {
                *length = *dimension;
                *dimension = mm_strdup("-1");
            }
            break;

        case ECPGt_char:
        case ECPGt_unsigned_char:
        case ECPGt_string:
            if (pointer_len == 2) {
                *length = *dimension = mm_strdup("0");
            } else if (pointer_len == 1) {
                *length = mm_strdup("0");
            }
            // Handle string length logic
            if (atoi(*length) < 0) {
                if (atoi(*dimension) < 0 && !type_definition)
                    *length = mm_strdup("1");
                else if (strcmp(*dimension, "0") == 0)
                    *length = mm_strdup("-1");
                else
                    *length = *dimension;
                *dimension = mm_strdup("-1");
            }
            break;

        default:
            if (pointer_len) {
                *length = *dimension;
                *dimension = mm_strdup("0");
            }
            if (atoi(*length) >= 0)
                mmfatal(PARSE_ERROR, "multidimensional arrays for simple data types are not supported");
            break;
    }
}
```