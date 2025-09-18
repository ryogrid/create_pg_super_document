# gather_boolean_expression

## Location
src/bin/psql/command.c: 3143 - 3172

## Overview
Collects and concatenates command-line arguments into a single boolean expression string for evaluation by psql conditional commands.

## Definition
```c
static PQExpBuffer gather_boolean_expression(PsqlScanState scan_state)
```

## Detailed Description
This function reads all remaining arguments from the current psql command line and combines them into a single space-separated string stored in a PQExpBuffer. It is designed to support psql conditional commands like `\if` by gathering the complete boolean expression that will later be parsed and evaluated. The function continues reading tokens until no more arguments are available, automatically inserting spaces between tokens to maintain proper expression formatting.

The function is intentionally simple and does not perform any validation of the gathered expression, leaving that responsibility to the downstream parsing functions like ParseVariableBool. The comments indicate this approach was chosen to allow for future extensions to the conditional command syntax.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer containing the current parsing state and input buffer for extracting command arguments

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer (to create the return buffer for the expression)
  - psql_scan_slash_option (to extract individual tokens from the command line)
  - appendPQExpBufferChar (to add space separators between tokens)
  - appendPQExpBufferStr (to append each token to the expression buffer)
  - free (to deallocate individual token strings)
- Called from (representative examples):
  - is_true_boolean_expression (to gather the expression before evaluation)
  - ignore_boolean_expression (to consume arguments when not evaluating)

## Notes and Other Information
- Returns a PQExpBuffer containing the complete boolean expression string
- Uses OT_NORMAL option type for standard token parsing
- Automatically handles spacing between tokens in the final expression
- The caller is responsible for destroying the returned PQExpBuffer
- Designed with extensibility in mind for future conditional command enhancements like "\\if defined VARNAME"
- Does not perform expression validation - delegates that to ParseVariableBool and similar functions
- Part of the psql conditional command processing infrastructure