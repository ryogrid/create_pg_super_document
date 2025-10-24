# output_get_descr

## Location
[src/interfaces/ecpg/preproc/descriptor.c:181-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L181-L213)

## Overview
Generates C code for retrieving individual item values from an SQL descriptor at a specified index position.

## Definition

```c
struct assignment *results;
```
## Detailed Description
This function is part of the ECPG preprocessor that generates runtime C code for SQL descriptor item operations. It processes assignments to retrieve specific descriptor item values and outputs the corresponding ECPGget_desc function call. The function handles various descriptor item types and generates appropriate type information for each variable assignment.

The generated code follows this pattern:
- Outputs the beginning of an ECPGget_desc call with descriptor name and index
- Processes each assignment in the global assignments list
- For each assignment, finds the target variable and generates type information
- Handles special cases for nullable and key_member items with warnings
- Uses get_dtype to map descriptor item types to runtime constants
- Uses ECPGdump_a_type to generate variable type information
- Terminates the descriptor list with ECPGd_EODT marker

## Parameters / Member Variables
- : The name of the SQL descriptor from which to retrieve item values
- : The index position of the descriptor item to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - struct assignment (assignment structure for descriptor operations)
  - [find_variable](../f/find_variable.md) (function to locate variable definitions)
  - [mm_strdup](../m/mm_strdup.md) (memory management string duplication)
  - ECPGd_nullable, ECPGd_key_member (descriptor item type constants)
  - mmerror (error reporting with PARSE_ERROR and ET_WARNING)
  - [get_dtype](../g/get_dtype.md) (function to convert descriptor types to runtime constants)
  - [ECPGdump_a_type](../E/ECPGdump_a_type.md) (function to generate variable type information)
  - [drop_assignments](../d/drop_assignments.md) (function to clean up assignment list)
  - [whenever_action](../w/whenever_action.md) (function to handle WHENEVER clause processing)
- Called from (representative examples):
  - No direct callers found in current analysis

## Notes and Other Information
- This function is part of the ECPG preprocessor code generation system
- It outputs to base_yyout, which is the main output stream for generated C code
- Special handling for nullable (always 1) and key_member (always 0) items with warnings
- The function uses a zero string for certain type dump operations
- ECPGd_EODT marks the end of the descriptor type list in generated code
- The whenever_action(2 | 1) call combines multiple error handling modes
- This handles individual descriptor items, complementing output_get_descr_header for header operations

## Simplified Source

```c
void output_get_descr(char *desc_name, char *index) {
    // Generate ECPGget_desc function call with descriptor name and index
    fprintf(base_yyout, "{ ECPGget_desc(__LINE__, %s, %s,", desc_name, index);

    // Process each assignment to get descriptor items
    for (struct assignment *results = assignments; results != NULL; results = results->next) {
        const struct variable *v = find_variable(results->variable);
        char *str_zero = mm_strdup("0");

        // Handle special cases with warnings
        switch (results->value) {
            case ECPGd_nullable:
                mmerror(PARSE_ERROR, ET_WARNING, "nullable is always 1");
                break;
            case ECPGd_key_member:
                mmerror(PARSE_ERROR, ET_WARNING, "key_member is always 0");
                break;
            default:
                break;
        }

        // Generate type information and variable dump
        fprintf(base_yyout, "%s,", get_dtype(results->value));
        ECPGdump_a_type(base_yyout, v->name, v->type, v->brace_level,
                        NULL, NULL, -1, NULL, NULL, str_zero, NULL, NULL);
        free(str_zero);
    }

    // Complete the descriptor call
    drop_assignments();
    fputs("ECPGd_EODT);\\n", base_yyout);
    whenever_action(2 | 1);
}
```