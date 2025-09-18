# output_set_descr_header

## Location
src/interfaces/ecpg/preproc/descriptor.c: 214 - 232

## Overview
Generates C code for setting header information in an SQL descriptor, specifically handling the assignment of the count value.

## Definition


## Detailed Description
This function is part of the ECPG preprocessor that generates runtime C code for SQL descriptor header modification operations. It processes assignments to set header information in an SQL descriptor and outputs the corresponding ECPGset_desc_header function call. Like its counterpart output_get_descr_header, it specifically handles the ECPGd_count descriptor header item for setting the number of items in the descriptor.

The generated code follows this pattern:
- Outputs the beginning of an ECPGset_desc_header call with the descriptor name
- Processes each assignment in the global assignments list
- For ECPGd_count assignments, generates code to set the count value using numeric lvalue handling
- Issues warnings for unsupported header items
- Completes the function call with proper casting and adds error handling

## Parameters / Member Variables
- : The name of the SQL descriptor for which to set header information

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
- Only ECPGd_count is currently supported as a settable descriptor header item
- The function includes explicit integer casting in the generated code: (int)(...)
- The function automatically cleans up the assignments list after processing
- The whenever_action(3) call handles SQL exception processing for the generated code
- This is the counterpart to output_get_descr_header, providing set functionality instead of get
- This is specifically for SQL descriptor header operations, not individual descriptor items