# BuildParamLogString

## Location
src/backend/nodes/params.c: 335 - 406

## Overview
Generates a human-readable string representation of parameter values for logging and error reporting purposes.

## Definition
char *BuildParamLogString(ParamListInfo params, char **knownTextValues, int maxlen)

## Detailed Description
This function creates a formatted string showing parameter values in the form '$1 = value, $2 = value, ...' for use in log messages and error contexts. It handles both known text representations (passed via knownTextValues) and automatically converts unknown parameters using their type's output functions. The function uses a temporary memory context to safely call output functions and prevents issues in aborted transactions.

The function cannot operate when parameter fetch hooks are active or during aborted transactions, returning NULL in these cases to avoid potential errors during error reporting.

## Parameters / Member Variables
- : The ParamListInfo containing parameters to format
- : Array of pre-computed text values (can contain NULLs for unknown values, or be NULL entirely)
- : Maximum length for individual parameter values (-1 for unlimited, longer values get ellipsis)

## Dependencies
- Functions called/Symbols referenced:
  - ParamListInfo (parameter type)
  - IsAbortedTransactionBlockState (to check transaction state)
  - AllocSetContextCreate (for temporary memory context)
  - ALLOCSET_DEFAULT_SIZES (memory context configuration)
  - ParamExternData (individual parameter structure)
  - appendStringInfoStringQuoted (for formatted output with quoting)
  - getTypeOutputInfo (to get type output function)
  - OidOutputFunctionCall (to convert values to text)
  - MemoryContextDelete (cleanup)
- Called from (representative examples):
  - ExplainQueryParameters (for EXPLAIN command output)
  - exec_bind_message (for query execution logging)
  - errdetail_params (for error message parameter details)

## Notes and Other Information
- Returns NULL if parameter fetch hooks are active or in aborted transactions
- Uses temporary memory context for safety during type output function calls
- The result string is allocated in the caller's memory context
- Supports both automatic type conversion and pre-computed text values
- Handles NULL and invalid type OID parameters gracefully
- Used to generate the paramValuesStr field in ParamListInfo structures