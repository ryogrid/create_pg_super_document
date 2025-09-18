# print_action

## Location
src/interfaces/ecpg/preproc/output.c: 37 - 65

## Overview
Outputs appropriate C code statements based on ECPG WHENEVER action types for error handling in embedded SQL programs.

## Definition
```c
static void print_action(struct when *w)
```

## Detailed Description
This static function generates corresponding C code for various ECPG WHENEVER action types. It takes a 'when' structure containing an action code and optional command, then outputs the appropriate C statement to the base output stream. The function supports standard ECPG error handling actions including SQLPRINT, GOTO, DO, STOP, BREAK, and CONTINUE, with a default case for unimplemented actions.

## Parameters / Member Variables
- `w`: Pointer to a 'when' structure containing:
  - `code`: Integer code indicating the type of action (W_SQLPRINT, W_GOTO, W_DO, W_STOP, W_BREAK, W_CONTINUE)
  - `command`: String command for actions that require additional parameters (e.g., GOTO label, DO statement)

## Dependencies
- Functions called/Symbols referenced:
  - fprintf
  - base_yyout (global output file pointer)
  - W_SQLPRINT, W_GOTO, W_DO, W_STOP, W_BREAK, W_CONTINUE (enumeration constants)
  - struct when
- Called from:
  - whenever_action (multiple locations in src/interfaces/ecpg/preproc/output.c)

## Notes and Other Information
- Static function, only accessible within the output.c module
- Part of the ECPG error handling mechanism for WHENEVER statements
- Generates different C code patterns based on the action type
- Includes fallback handling for unimplemented action codes
- Essential for translating ECPG WHENEVER directives into executable C code