# output_get_descr_header

## Location
[src/interfaces/ecpg/preproc/descriptor.c:162-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L162-L180)

## Overview
Generates C code for retrieving header information from an SQL descriptor, specifically handling the count of items in the descriptor.

## Definition

```c
struct assignment *results;
```
## Detailed Description
This function is part of the ECPG preprocessor that generates runtime C code for SQL descriptor operations. It processes assignments to retrieve header information from an SQL descriptor and outputs the corresponding ECPGget_desc_header function call. The function specifically handles the ECPGd_count descriptor header item, which represents the number of items in the descriptor.

The generated code follows this pattern:
- Outputs the beginning of an ECPGget_desc_header call with the descriptor name
- Processes each assignment in the global assignments list
- For ECPGd_count assignments, generates code to retrieve the count value
- Issues warnings for unsupported header items
- Completes the function call and adds error handling

## Parameters / Member Variables
- : The name of the SQL descriptor from which to retrieve header information

## Dependencies
- Functions called/Symbols referenced:
  - struct assignment (assignment structure for descriptor operations)
  - ECPGd_count (enumeration value for descriptor count)
  - [ECPGnumeric_lvalue](../E/ECPGnumeric_lvalue.md) (function to handle numeric left-values)
  - mmerror (error reporting with PARSE_ERROR and ET_WARNING)
  - [drop_assignments](../d/drop_assignments.md) (function to clean up assignment list)
  - [whenever_action](../w/whenever_action.md) (function to handle WHENEVER clause processing)
- Called from (representative examples):
  - No direct callers found in current analysis

## Notes and Other Information
- This function is part of the ECPG preprocessor code generation system
- It outputs to base_yyout, which is the main output stream for generated C code
- Only ECPGd_count is currently supported as a descriptor header item
- The function automatically cleans up the assignments list after processing
- The whenever_action(3) call handles SQL exception processing for the generated code
- This is specifically for SQL descriptor header operations, not individual descriptor items

## Simplified Source

```c
void output_get_descr_header(char *desc_name) {
    // Generate ECPGget_desc_header function call
    fprintf(base_yyout, "{ ECPGget_desc_header(__LINE__, %s, &(", desc_name);

    // Process each assignment in the list
    for (struct assignment *results = assignments; results != NULL; results = results->next) {
        if (results->value == ECPGd_count) {
            // Handle count assignment - generate numeric lvalue code
            ECPGnumeric_lvalue(results->variable);
        } else {
            // Warn about unsupported header items
            mmerror(PARSE_ERROR, ET_WARNING, "descriptor header item \"%d\" does not exist", results->value);
        }
    }

    // Complete the function call and add error handling
    drop_assignments();
    fprintf(base_yyout, "));\\n");
    whenever_action(3);
}
```