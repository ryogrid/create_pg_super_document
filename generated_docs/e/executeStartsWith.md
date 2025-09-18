# executeStartsWith

## Location
[src/backend/utils/adt/jsonpath_exec.c:2243-2266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2243-L2266)

## Overview
A JSON path predicate callback function that checks if a string value starts with another specified string.

## Definition


## Detailed Description
The  function implements the STARTS_WITH predicate functionality for JSON path expressions in PostgreSQL. It performs a string prefix comparison operation by checking if the 'whole' string begins with the 'initial' string. The function handles JSON string values and performs binary comparison using  for efficiency. Both input values are validated and converted to string scalars before comparison.

## Parameters / Member Variables
- : JsonPathItem pointer (currently unused in the implementation)
- : JsonbValue pointer representing the string to be checked
- : JsonbValue pointer representing the prefix string to match against
- : void pointer for additional parameters (currently unused)

## Dependencies
- Functions called/Symbols referenced:
  - [getScalar](../g/getScalar.md): Converts JsonbValue to scalar string type
  - memcmp: Performs binary memory comparison
  - JsonPathItem: JSON path item structure
  - [JsonbValue](../J/JsonbValue.md): JSON binary value structure
  - jbvString: String type identifier for JSON values
- Called from (representative examples):
  - [executeBoolItem](executeBoolItem.md): Main boolean item execution function
  - RETURN_ERROR: Error handling macro

## Notes and Other Information
- Returns JsonPathBool values: jpbTrue if string starts with prefix, jpbFalse otherwise, jpbUnknown on error
- Performs length checking to avoid buffer overruns before calling memcmp
- Both input values must be convertible to string scalars; non-string values result in jpbUnknown (error)
- Uses binary comparison which is case-sensitive and encoding-aware
- Part of PostgreSQL's JSON path expression evaluation system