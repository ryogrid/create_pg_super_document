# whenever_action

## Location
src/interfaces/ecpg/preproc/output.c: 66 - 93

## Overview
Generates C code for ECPG WHENEVER error handling based on the current mode and active WHENEVER conditions.

## Definition
```c
void whenever_action(int mode)
```

## Detailed Description
This function generates appropriate error handling code based on the ECPG WHENEVER directives that are currently active. It checks three types of SQL conditions (NOT_FOUND, WARNING, and ERROR) and outputs corresponding C conditional statements with their associated actions. The mode parameter controls specific behaviors like whether to handle NOT_FOUND conditions and whether to close code blocks. This function is central to ECPG's error handling mechanism.

## Parameters / Member Variables
- `mode`: Integer bitmask controlling behavior:
  - Bit 0 (mode & 1): If set, handle NOT_FOUND conditions
  - Bit 1 (mode & 2): If set, output closing brace for code block

## Dependencies
- Functions called/Symbols referenced:
  - output_line_number
  - fprintf
  - print_action
  - fputc
  - base_yyout (global output file pointer)
  - W_NOTHING (enumeration constant)
  - when_nf, when_warn, when_error (global when structures)
- Called from:
  - output_get_descr_header (src/interfaces/ecpg/preproc/descriptor.c:177)
  - output_get_descr (src/interfaces/ecpg/preproc/descriptor.c:210)
  - output_set_descr_header (src/interfaces/ecpg/preproc/descriptor.c:229)
  - output_set_descr (src/interfaces/ecpg/preproc/descriptor.c:326)
  - output_simple_statement (src/interfaces/ecpg/preproc/output.c:23)
  - output_statement (src/interfaces/ecpg/preproc/output.c:165)
  - output_prepare_statement (src/interfaces/ecpg/preproc/output.c:177)
  - output_deallocate_prepare_statement (src/interfaces/ecpg/preproc/output.c:195)

## Notes and Other Information
- Core component of ECPG's WHENEVER statement implementation
- Generates SQL condition checks using sqlca (SQL Communication Area)
- Handles three standard SQL condition types: NOT_FOUND, WARNING, and ERROR
- Uses global when_* structures to track active WHENEVER directives
- Maintains proper line number correspondence in generated code
- The mode parameter provides flexible control over when and how error handling is applied