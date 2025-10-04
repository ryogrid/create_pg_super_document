# PGTYPESnumeric_from_asc

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:321-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L321-L342)

## Overview
Converts a string representation of a numeric value into a PostgreSQL numeric type structure for use in ECPG applications.

## Definition
```c
numeric *PGTYPESnumeric_from_asc(char *str, char **endptr)
```

## Detailed Description
This function parses a string containing a numeric value and creates a PostgreSQL numeric type structure. It allocates memory for a new numeric structure and uses the internal `set_var_from_str` function to parse the string and populate the numeric value. The function handles memory allocation failures and parsing errors gracefully by returning NULL and cleaning up allocated memory when necessary.

## Parameters / Member Variables
- `str`: Input string containing the numeric value to be parsed
- `endptr`: Optional pointer to a char pointer that will be set to point to the first character after the parsed numeric value; if NULL, an internal pointer is used instead

## Dependencies
- Functions called/Symbols referenced:
  - [pgtypes_alloc](../p/pgtypes_alloc.md) (for memory allocation)
  - [set_var_from_str](../s/set_var_from_str.md) (for string parsing)
  - [PGTYPESnumeric_free](PGTYPESnumeric_free.md) (for cleanup on error)
  - [numeric](../n/numeric.md) (type definition)
- Called from (representative examples):
  - [deccvasc](../d/deccvasc.md) (Informix compatibility function)
  - [ecpg_get_data](../e/ecpg_get_data.md) (ECPG data retrieval)
  - [PGTYPESnumeric_from_double](PGTYPESnumeric_from_double.md) (numeric conversion)
  - Various test functions

## Notes and Other Information
- Returns NULL on memory allocation failure or parsing error
- Automatically cleans up allocated memory if parsing fails
- Part of the ECPG pgtypes library for PostgreSQL embedded SQL
- The endptr parameter follows the same convention as standard C library functions like strtol()
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:321-342

## Simplified Source

```c
numeric *PGTYPESnumeric_from_asc(char *str, char **endptr) {
    // Allocate memory for numeric structure
    numeric *value = (numeric *) pgtypes_alloc(sizeof(numeric));
    if (!value)
        return NULL;

    // Set up end pointer handling
    char *realptr;
    char **ptr = (endptr != NULL) ? endptr : &realptr;

    // Parse string into numeric value
    int ret = set_var_from_str(str, ptr, value);
    if (ret) {
        PGTYPESnumeric_free(value);
        return NULL;
    }

    return value;
}
```