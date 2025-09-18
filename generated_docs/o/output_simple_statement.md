# output_simple_statement

## Location
src/interfaces/ecpg/preproc/output.c: 19 - 36

## Overview
Outputs a simple SQL statement to the preprocessor output stream with optional error handling and line number tracking.

## Definition
```c
void output_simple_statement(char *stmt, int whenever_mode)
```

## Detailed Description
This function processes and outputs a simple SQL statement to the ECPG preprocessor output. It first outputs the statement string in escaped form, then optionally handles error conditions based on the whenever_mode parameter, follows with a line number directive for debugging, and finally cleans up the allocated statement memory. This function is part of the ECPG statement processing pipeline.

## Parameters / Member Variables
- `stmt`: A dynamically allocated string containing the SQL statement to output
- `whenever_mode`: An integer flag indicating the type of error handling to apply (0 for none, non-zero for specific error handling modes)

## Dependencies
- Functions called/Symbols referenced:
  - output_escaped_str
  - whenever_action
  - output_line_number
  - free
- Called from:
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Part of the ECPG (Embedded SQL in C) preprocessor system
- Handles memory management by freeing the input statement string
- Integrates error handling through the whenever_mode parameter
- Maintains source line correspondence through output_line_number()
- The function assumes ownership of the stmt parameter and frees it