# xmlvalidate

## Location
[src/backend/utils/adt/xml.c:1119-1128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1119-L1128)

## Overview
The xmlvalidate function was intended to validate XML documents against DTD schemas but has been removed for security reasons and now always throws an error when called.

## Definition
```c
Datum xmlvalidate(PG_FUNCTION_ARGS)
```

## Detailed Description
This function was originally designed to validate XML documents against a Document Type Definition (DTD) provided as an external link. However, it has been permanently disabled and removed from functionality due to serious security concerns. The function now immediately raises an error with the message "xmlvalidate is not implemented" whenever it is called. The security issue arose because allowing unprivileged users to specify external DTD URLs would enable PostgreSQL to fetch arbitrary external files, creating a potential attack vector for accessing unauthorized resources.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (function was intended to take XML document and DTD parameters)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - ERROR (error level constant)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - ERRCODE_FEATURE_NOT_SUPPORTED (specific error code)
- Called from (representative examples):
  - No current callers (function is disabled)

## Notes and Other Information
- This function is permanently disabled for security reasons and cannot be re-enabled
- The security vulnerability was that unprivileged users could cause PostgreSQL to fetch arbitrary external files through DTD parameter
- Any attempt to use the XMLVALIDATE SQL function will result in an error
- The function remains in the codebase for compatibility but is non-functional
- Alternative XML validation approaches should be implemented outside of PostgreSQL if needed

## Simplified Source

```c
Datum
xmlvalidate(PG_FUNCTION_ARGS)
{
    // Function disabled for security reasons - always throws error
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("xmlvalidate is not implemented")));
    return 0;
}
```