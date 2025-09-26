# when

## Location
src/interfaces/ecpg/preproc/type.h: 87 - 93

## Overview
The 'when' struct represents action handlers for WHENEVER statements in ECPG, defining what action to take when specific SQL conditions (errors, warnings, not found) occur.

## Definition


## Detailed Description
The 'when' struct is a core component of ECPG's WHENEVER statement mechanism, which provides error handling capabilities for embedded SQL programs. WHENEVER statements allow developers to specify actions that should be taken automatically when certain SQL conditions arise, such as SQL errors, warnings, or 'not found' conditions.

Each 'when' struct instance represents one specific handler configuration. The ECPG preprocessor maintains global instances (when_error, when_nf, when_warn) that define the current action for each type of SQL condition. When SQL operations are executed, the appropriate handler is consulted to determine what action should be taken.

The structure supports various predefined actions (CONTINUE, BREAK, STOP, etc.) as well as custom actions (GOTO with a label, or DO with custom code), providing flexible error handling strategies for embedded SQL applications.

## Parameters / Member Variables
- : A WHEN_TYPE enumeration value specifying the type of action (W_CONTINUE, W_BREAK, W_STOP, W_GOTO, W_DO, W_SQLPRINT, W_NOTHING)
- : String containing the specific command or label for certain action types (e.g., label name for W_GOTO, code for W_DO)
- : Additional string parameter used for certain action types or descriptive purposes

## Dependencies
- Functions called/Symbols referenced:
  - WHEN_TYPE (enumeration defining action types)
- Called from (representative examples):
  - print_action (in output.c for code generation)
  - main (in ecpg.c for initialization)
  - output_simple_statement (in output.c)
  - Global instances: when_error, when_nf, when_warn

## Notes and Other Information
- Three global instances are maintained: when_error (for SQLERROR), when_nf (for NOT FOUND), and when_warn (for SQLWARNING)  
- The structure is initialized with memset() to ensure clean state
- Used primarily during code generation phase to inject appropriate error handling code
- Part of the ECPG WHENEVER statement implementation which provides automatic error handling in embedded SQL
- The WHEN_TYPE enumeration includes values like W_NOTHING (no action), W_CONTINUE (continue execution), W_BREAK (break from loop), W_STOP (terminate program), W_GOTO (jump to label), W_DO (execute custom code), and W_SQLPRINT (print SQL error)
- Memory for command and str fields is dynamically allocated and managed by the ECPG preprocessor