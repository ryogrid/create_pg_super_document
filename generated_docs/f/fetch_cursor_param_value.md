# fetch_cursor_param_value

## Location
[src/backend/executor/execCurrent.c:258-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execCurrent.c#L258-L313)

## Overview
Fetches the string value of a parameter from the execution context, specifically verifying that it is of REFCURSOR type for use in cursor operations.

## Definition

```c
static char *
fetch_cursor_param_value(ExprContext *econtext, int paramId)
```
## Detailed Description
This static helper function retrieves parameter values when cursor names are specified as parameters rather than literal strings in CURRENT OF expressions. It performs several critical validations to ensure type safety and parameter validity:

The function first checks if parameter information is available in the execution context and if the requested parameter ID is within valid bounds. It handles dynamic parameters by calling the paramFetch hook if one is registered, otherwise accessing the parameter directly from the params array.

Type safety is enforced by verifying that the parameter is of REFCURSOR type (OID REFCURSOROID). If the types don't match, it reports a detailed error indicating both the actual and expected types. For valid REFCURSOR parameters, it extracts the string value using text I/O routines since REFCURSOR uses the same representation as the text type.

## Parameters / Member Variables
- : Expression context containing parameter list information
- : 1-based parameter ID to fetch (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - [ParamListInfo](../P/ParamListInfo.md) (parameter list structure)
  - ParamExternData (individual parameter data structure)
  - TextDatumGetCString (to convert REFCURSOR datum to C string)
  - OidIsValid (to validate parameter type OID)
  - [format_type_be](format_type_be.md) (for error message formatting)
- Called from (representative examples):
  - [execCurrentOf](../e/execCurrentOf.md) (when cursor name is parameterized)

## Notes and Other Information
The function is marked static as it's only used within execCurrent.c. It raises ERRCODE_DATATYPE_MISMATCH for type mismatches and ERRCODE_UNDEFINED_OBJECT when no value is found for the specified parameter. The 1-based parameter indexing matches PostgreSQL's parameter numbering convention. The function handles the paramFetch hook mechanism which allows for dynamic parameter resolution in prepared statements and other contexts.