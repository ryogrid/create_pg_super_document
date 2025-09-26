# variable

## Location
[src/interfaces/ecpg/ecpglib/ecpglib_extern.h:138-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/ecpglib_extern.h#L138-L154)

## Overview
The `variable` struct represents variables and their associated indicator variables in ECPG, managing both data values and metadata for embedded SQL operations.

## Definition
```c
struct variable
{
    enum ECPGttype type;
    void       *value;
    void       *pointer;
    long        varcharsize;
    long        arrsize;
    long        offset;
    enum ECPGttype ind_type;
    void       *ind_value;
    void       *ind_pointer;
    long        ind_varcharsize;
    long        ind_arrsize;
    long        ind_offset;
    struct variable *next;
};
```

## Detailed Description
This structure represents variables used in embedded SQL statements, providing complete metadata for both the main data variable and its optional indicator variable. It supports various data types, arrays, and complex structures, enabling comprehensive data exchange between C host variables and SQL statements. The structure forms a linked list to support multiple variables in a single SQL operation, and includes detailed size and offset information for proper memory management.

## Parameters / Member Variables
- `type`: ECPG type enumeration indicating the data type of the main variable
- `value`: Pointer to the actual data value of the main variable
- `pointer`: Additional pointer for complex data types requiring indirection
- `varcharsize`: Size specification for variable-length character data
- `arrsize`: Array size for array-type variables (number of elements)
- `offset`: Memory offset for accessing elements within structures or arrays
- `ind_type`: ECPG type enumeration for the associated indicator variable
- `ind_value`: Pointer to the indicator variables data value
- `ind_pointer`: Additional pointer for complex indicator variable types
- `ind_varcharsize`: Size specification for variable-length indicator character data
- `ind_arrsize`: Array size for indicator variable arrays
- `ind_offset`: Memory offset for accessing indicator variable elements
- `next`: Pointer to the next variable in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - ECPGttype (enumeration for ECPG data types)
- Called from (representative examples):
  - No direct references found in the current analysis

## Notes and Other Information
This structure is central to ECPGs variable management system, supporting the full range of PostgreSQL data types and their C language equivalents. The dual nature of the structure (main variable and indicator variable) follows SQL standards for NULL handling and data status indication. The comprehensive metadata enables proper data conversion, memory allocation, and array handling for complex embedded SQL operations.