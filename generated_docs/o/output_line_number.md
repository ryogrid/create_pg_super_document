# output_line_number

## Location
[src/interfaces/ecpg/preproc/output.c:10-18](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/output.c#L10-L18)

## Overview
Outputs a line number directive to the preprocessor output stream for debugging and error tracking purposes in ECPG-generated code.

## Definition
```c
void output_line_number(void)
```

## Detailed Description
This function generates and outputs a C preprocessor line number directive to the base output stream. It calls `hashline_number()` to create the formatted line directive string and writes it to `base_yyout`. The function is essential for maintaining source code line number correspondence between the original ECPG input and the generated C code, which is crucial for debugging and error reporting.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [hashline_number](../h/hashline_number.md)
  - fprintf
  - free
  - base_yyout (global output file pointer)
- Called from:
  - [main](../m/main.md) (src/interfaces/ecpg/preproc/ecpg.c:480)
  - [output_simple_statement](output_simple_statement.md) (src/interfaces/ecpg/preproc/output.c:24)
  - [whenever_action](../w/whenever_action.md) (multiple locations)

## Notes and Other Information
- Part of the ECPG (Embedded SQL in C) preprocessor system
- Memory management is handled properly by freeing the allocated line string
- Essential for maintaining source line correspondence in generated code
- Used extensively in error handling and statement output contexts