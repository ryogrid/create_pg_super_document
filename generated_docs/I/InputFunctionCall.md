# InputFunctionCall

## Location
[src/backend/utils/fmgr/fmgr.c:1530-1584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1530-L1584)

## Overview
InputFunctionCall is a convenience wrapper that calls a previously-looked-up datatype input function to convert a string representation to its internal Datum format.

## Definition

```c
struct and
 * returning false.  (The caller can choose to test SOFT_ERROR_OCCURRED(),
 * but checking the function result instead is usually cheaper.)
 *
 * If escontext does not point to an ErrorSaveContext, errors are reported
 * via ereport(ERROR), so that there is no functional difference from
 * InputFunctionCall;
```
## Detailed Description
This function provides a convenient interface for calling PostgreSQL's datatype input functions. It handles the setup of function call information and manages NULL value processing according to the function's strictness. The function takes a string representation of a value and converts it to the appropriate internal Datum representation using the specified input function. It includes error checking to ensure that NULL inputs produce NULL outputs and non-NULL inputs produce non-NULL outputs, maintaining data consistency.

## Parameters / Member Variables
- : Function manager info structure containing details about the input function to call
- : String representation of the value to convert (may be NULL to indicate a NULL value)
- : OID parameter passed to the input function (type-specific parameter)
- : Type modifier value providing additional type information

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info)
  - InitFunctionCallInfoData (initializes function call structure)
  - [CStringGetDatum](../C/CStringGetDatum.md) (converts C string to Datum)
  - FunctionCallInvoke (invokes the actual function)
- Called from (representative examples):
  - [BuildTupleFromCStrings](../B/BuildTupleFromCStrings.md)
  - [XmlTableGetValue](../X/XmlTableGetValue.md)
  - [OidInputFunctionCall](../O/OidInputFunctionCall.md)
  - [plperl_sv_to_datum](../p/plperl_sv_to_datum.md)
  - [PLyObject_ToScalar](../P/PLyObject_ToScalar.md)

## Notes and Other Information
- Handles NULL input specially: if str is NULL and the function is strict, returns NULL without calling the function
- Includes validation that NULL inputs produce NULL results and non-NULL inputs produce non-NULL results
- Part of PostgreSQL's function manager (fmgr) subsystem for type I/O operations
- Used extensively in procedural languages (PL/Perl, PL/Python, PL/Tcl) for data type conversions

## Simplified Source

```c
// Simplified version of InputFunctionCall
Datum InputFunctionCall(FmgrInfo *flinfo, char *str, Oid typioparam, int32 typmod) {
    LOCAL_FCINFO(fcinfo, 3);
    Datum result;

    // Handle NULL input for strict functions
    if (str == NULL && flinfo->fn_strict) {
        return (Datum) 0;  // Return NULL without calling function
    }

    // Set up function call with 3 arguments
    InitFunctionCallInfoData(*fcinfo, flinfo, 3, InvalidOid, NULL, NULL);

    // Prepare arguments: string, type IO param, type modifier
    fcinfo->args[0].value = CStringGetDatum(str);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = ObjectIdGetDatum(typioparam);
    fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = Int32GetDatum(typmod);
    fcinfo->args[2].isnull = false;

    // Call the input function
    result = FunctionCallInvoke(fcinfo);

    // Validate NULL handling consistency
    if (str == NULL) {
        // NULL input should produce NULL result
        if (!fcinfo->isnull) {
            elog(ERROR, "input function %u returned non-NULL", flinfo->fn_oid);
        }
    } else {
        // Non-NULL input should produce non-NULL result
        if (fcinfo->isnull) {
            elog(ERROR, "input function %u returned NULL", flinfo->fn_oid);
        }
    }

    return result;
}
```

Key simplifications made:
- Added clear comments explaining each phase of the function call
- Simplified the argument setup with explanatory comments
- Highlighted the NULL validation logic and its purpose
- Focused on the main execution path while preserving error checking