# BuildParamLogString

## Location
[src/backend/nodes/params.c:335-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/params.c#L335-L406)

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
  - [ParamListInfo](../P/ParamListInfo.md) (parameter type)
  - [IsAbortedTransactionBlockState](../I/IsAbortedTransactionBlockState.md) (to check transaction state)
  - AllocSetContextCreate (for temporary memory context)
  - ALLOCSET_DEFAULT_SIZES (memory context configuration)
  - ParamExternData (individual parameter structure)
  - [appendStringInfoStringQuoted](../a/appendStringInfoStringQuoted.md) (for formatted output with quoting)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md) (to get type output function)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md) (to convert values to text)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (cleanup)
- Called from (representative examples):
  - [ExplainQueryParameters](../E/ExplainQueryParameters.md) (for EXPLAIN command output)
  - [exec_bind_message](../e/exec_bind_message.md) (for query execution logging)
  - [errdetail_params](../e/errdetail_params.md) (for error message parameter details)

## Notes and Other Information
- Returns NULL if parameter fetch hooks are active or in aborted transactions
- Uses temporary memory context for safety during type output function calls
- The result string is allocated in the caller's memory context
- Supports both automatic type conversion and pre-computed text values
- Handles NULL and invalid type OID parameters gracefully
- Used to generate the paramValuesStr field in ParamListInfo structures

## Simplified Source

```c
// Simplified version of BuildParamLogString
char *BuildParamLogString(ParamListInfo params, char **knownTextValues, int maxlen) {
    StringInfoData buf;

    // Early exit conditions: can't process in certain states
    if (params->paramFetch != NULL || IsAbortedTransactionBlockState()) {
        return NULL;
    }

    // Initialize output buffer
    initStringInfo(&buf);

    // Create temporary memory context for safe type output function calls
    MemoryContext tmpCxt = AllocSetContextCreate(CurrentMemoryContext,
                                                "BuildParamLogString",
                                                ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCxt = MemoryContextSwitchTo(tmpCxt);

    // Process each parameter: format as "$1 = value, $2 = value, ..."
    for (int paramno = 0; paramno < params->numParams; paramno++) {
        ParamExternData *param = &params->params[paramno];

        // Add parameter number prefix
        appendStringInfo(&buf, "%s$%d = ",
                        paramno > 0 ? ", " : "",
                        paramno + 1);

        // Handle NULL or invalid type parameters
        if (param->isnull || !OidIsValid(param->ptype)) {
            appendStringInfoString(&buf, "NULL");
        } else {
            // Use known text value if available, otherwise convert from type
            if (knownTextValues != NULL && knownTextValues[paramno] != NULL) {
                appendStringInfoStringQuoted(&buf, knownTextValues[paramno], maxlen);
            } else {
                // Convert parameter value to string using type's output function
                Oid typoutput;
                bool typisvarlena;
                getTypeOutputInfo(param->ptype, &typoutput, &typisvarlena);
                char *pstring = OidOutputFunctionCall(typoutput, param->value);
                appendStringInfoStringQuoted(&buf, pstring, maxlen);
            }
        }
    }

    // Cleanup temporary memory context
    MemoryContextSwitchTo(oldCxt);
    MemoryContextDelete(tmpCxt);

    return buf.data;
}
```

Key simplifications made:
- Removed detailed comments while keeping essential logic comments
- Consolidated variable declarations closer to usage
- Simplified the branching logic structure for parameter processing
- Focused on the main execution path: initialize buffer, process each parameter, cleanup
- Preserved critical error handling (transaction state, fetch hooks)
- Maintained proper memory context management for safety