# JsonPathExecContext

## Location
src/backend/utils/adt/jsonpath_exec.c: 98 - 119

## Overview
The primary execution context structure for PostgreSQL's JSON path evaluation engine, containing all state information needed during path expression execution.

## Definition
```c
typedef struct JsonPathExecContext
{
    void           *vars;                   /* variables to substitute into jsonpath */
    JsonPathGetVarCallback getVar;          /* callback to extract a given variable from 'vars' */
    JsonbValue     *root;                   /* for $ evaluation */
    JsonbValue     *current;                /* for @ evaluation */
    JsonBaseObjectInfo baseObject;          /* "base object" for .keyvalue() evaluation */
    int             lastGeneratedObjectId;  /* "id" counter for .keyvalue() evaluation */
    int             innermostArraySize;     /* for LAST array index evaluation */
    bool            laxMode;                /* true for "lax" mode, false for "strict" mode */
    bool            ignoreStructuralErrors; /* with "true" structural errors such as absence of required json item or unexpected json item type are ignored */
    bool            throwErrors;            /* with "false" all suppressible errors are suppressed */
    bool            useTz;
} JsonPathExecContext;
```

## Detailed Description
JsonPathExecContext serves as the central state container for executing JSON path expressions in PostgreSQL. It maintains references to the root and current JSON values, handles variable substitution, tracks execution modes (lax vs strict), and manages error handling behavior. The context is threaded through all path execution functions to provide consistent access to execution state and configuration.

## Parameters / Member Variables
- `vars`: Pointer to variables available for substitution in the JSON path expression
- `getVar`: Callback function to extract specific variables from the vars structure
- `root`: JsonbValue pointer representing the root JSON document ($ reference)
- `current`: JsonbValue pointer representing the current context item (@ reference)
- `baseObject`: JsonBaseObjectInfo structure for tracking base objects during .keyvalue() evaluation
- `lastGeneratedObjectId`: Counter for generating unique IDs during .keyvalue() method execution
- `innermostArraySize`: Size of the innermost array, used for LAST array index evaluation
- `laxMode`: Boolean flag controlling lax vs strict execution mode
- `ignoreStructuralErrors`: When true, structural errors like missing items or type mismatches are ignored
- `throwErrors`: When false, all suppressible errors are suppressed instead of thrown
- `useTz`: Boolean flag controlling timezone usage in date/time operations

## Dependencies
- Functions called/Symbols referenced:
  - [JsonBaseObjectInfo](JsonBaseObjectInfo.md)
  - JsonPathGetVarCallback
  - [JsonbValue](JsonbValue.md)
- Called from (representative examples):
  - [executeJsonPath](../e/executeJsonPath.md)
  - [executeItem](../e/executeItem.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeBoolItem](../e/executeBoolItem.md)
  - [executeKeyValueMethod](../e/executeKeyValueMethod.md)

## Notes and Other Information
- This context structure is passed through nearly all JSON path execution functions
- The lax vs strict mode significantly affects error handling and type coercion behavior
- [Variable](../V/Variable.md) substitution mechanism allows for parameterized JSON path queries
- The baseObject and lastGeneratedObjectId are specifically for supporting the .keyvalue() method
- Error handling is highly configurable through the various boolean flags