# output_set_descr

## Location
[src/interfaces/ecpg/preproc/descriptor.c:275-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L275-L334)

## Overview
Generates C code for setting descriptor items in ECPG (Embedded SQL in C for PostgreSQL), processing assignment statements and outputting appropriate ECPGset_desc function calls.

## Definition

```c
struct assignment *results;
```
## Detailed Description
The `output_set_descr` function is part of the ECPG preprocessor that handles SQL descriptor SET operations. It processes a global list of assignments and generates corresponding C code that calls the ECPGset_desc runtime function. The function validates descriptor items, ensuring that read-only items cannot be set and unimplemented items are properly flagged. For valid settable items (data, indicator, length, type), it generates appropriate type information and variable references in the output C code.

## Parameters / Member Variables
- `desc_name`: Name of the SQL descriptor to be set
- `index`: Index position within the descriptor (can be a variable or literal)

## Dependencies
- Functions called/Symbols referenced:
  - [find_variable](../f/find_variable.md)
  - [descriptor_item_name](../d/descriptor_item_name.md)
  - [get_dtype](../g/get_dtype.md)
  - [ECPGdump_a_type](../E/ECPGdump_a_type.md)
  - [mm_strdup](../m/mm_strdup.md)
  - [drop_assignments](../d/drop_assignments.md)
  - [whenever_action](../w/whenever_action.md)
  - mmfatal
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- The function handles different types of descriptor items with specific validation:
  - Unimplemented items (cardinality, di_code, di_precision, precision, scale) trigger fatal errors
  - Read-only items (key_member, name, nullable, octet, ret_length, ret_octet) cannot be set
  - Settable items (data, indicator, length, type) are processed and output as ECPGset_desc calls
- Uses global `assignments` list to track descriptor assignments
- Outputs to `base_yyout` file stream as part of the preprocessing phase
- Calls `whenever_action(2 | 1)` to handle error conditions according to WHENEVER statements
- Part of the ECPG preprocessor located in src/interfaces/ecpg/preproc/descriptor.c:275-334

## Simplified Source

```c
void output_set_descr(char *desc_name, char *index) {
    // Generate ECPGset_desc function call
    fprintf(base_yyout, "{ ECPGset_desc(__LINE__, %s, %s,", desc_name, index);

    // Process each assignment to set descriptor items
    for (struct assignment *results = assignments; results != NULL; results = results->next) {
        const struct variable *v = find_variable(results->variable);

        switch (results->value) {
            // Unimplemented items - fatal error
            case ECPGd_cardinality:
            case ECPGd_di_code:
            case ECPGd_di_precision:
            case ECPGd_precision:
            case ECPGd_scale:
                mmfatal(PARSE_ERROR, "descriptor item \"%s\" is not implemented",
                        descriptor_item_name(results->value));
                break;

            // Read-only items - cannot be set
            case ECPGd_key_member:
            case ECPGd_name:
            case ECPGd_nullable:
            case ECPGd_octet:
            case ECPGd_ret_length:
            case ECPGd_ret_octet:
                mmfatal(PARSE_ERROR, "descriptor item \"%s\" cannot be set",
                        descriptor_item_name(results->value));
                break;

            // Settable items - generate type information
            case ECPGd_data:
            case ECPGd_indicator:
            case ECPGd_length:
            case ECPGd_type: {
                char *str_zero = mm_strdup("0");
                fprintf(base_yyout, "%s,", get_dtype(results->value));
                ECPGdump_a_type(base_yyout, v->name, v->type, v->brace_level,
                               NULL, NULL, -1, NULL, NULL, str_zero, NULL, NULL);
                free(str_zero);
                break;
            }
            default:
                break;
        }
    }

    // Complete the descriptor call
    drop_assignments();
    fputs("ECPGd_EODT);\\n", base_yyout);
    whenever_action(2 | 1);
}
```