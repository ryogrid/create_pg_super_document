# executeJsonPath

## Location
[src/backend/utils/adt/jsonpath_exec.c:679-734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L679-L734)

## Overview
Core internal function that serves as the main interface to the JSONPath executor, responsible for executing JSONPath expressions against JSONB documents and collecting results.

## Definition
```c
static JsonPathExecResult executeJsonPath(JsonPath *path, void *vars, 
                                         JsonPathGetVarCallback getVar,
                                         JsonPathCountVarsCallback countVars,
                                         Jsonb *json, bool throwErrors, 
                                         JsonValueList *result, bool useTz)
```

## Detailed Description
This function is the central orchestrator for JSONPath execution in PostgreSQL. It sets up the execution context, initializes the JSONPath parser, and manages the execution environment for processing JSONPath expressions against JSONB documents. The function handles both strict and lax execution modes, manages variable substitution, and coordinates timezone-aware processing.

Key responsibilities include:
- Initializing the JSONPath execution context with proper configuration
- Setting up variable callbacks for dynamic value substitution  
- Managing execution modes (strict vs lax) and error handling policies
- Coordinating the recursive execution through `executeItem`
- Handling timezone-aware operations when required
- Optimizing execution for existence-only checks vs full result collection

## Parameters / Member Variables
- `path`: JSONPath expression to be executed
- `vars`: Variables context for substitution in the JSONPath expression
- `getVar`: Callback function to extract variable values from the vars context
- `countVars`: Callback function to count the number of variables in the context
- `json`: Target JSONB document for path evaluation
- `throwErrors`: Boolean flag determining whether suppressible errors should be thrown
- `result`: Output list to store matching result items (NULL for existence-only checks)
- `useTz`: Boolean flag enabling timezone-aware datetime operations

## Dependencies
- Functions called/Symbols referenced:
  - [JsonPathExecContext](../J/JsonPathExecContext.md) (execution context structure)
  - [JsonPathExecResult](../J/JsonPathExecResult.md) (return type enumeration)
  - JsonPathItem (path parsing structure)
  - [JsonbValue](../J/JsonbValue.md) (JSONB value representation)
  - [jspInit](../j/jspInit.md) (JSONPath parser initialization)
  - [JsonbExtractScalar](../J/JsonbExtractScalar.md) (JSONB scalar extraction)
  - [JsonbInitBinary](../J/JsonbInitBinary.md) (JSONB binary initialization)
  - jspStrictAbsenceOfErrors (strict mode checking)
  - [executeItem](executeItem.md) (recursive item execution)
  - jperIsError (error result checking)
  - [JsonValueListIsEmpty](../J/JsonValueListIsEmpty.md) (result list checking)
- Called from (representative examples):
  - [jsonb_path_exists_internal](../j/jsonb_path_exists_internal.md)
  - [jsonb_path_query_internal](../j/jsonb_path_query_internal.md)
  - [jsonb_path_query_first_internal](../j/jsonb_path_query_first_internal.md)
  - [jsonb_path_query_array_internal](../j/jsonb_path_query_array_internal.md)
  - [JsonPathExists](../J/JsonPathExists.md)
  - [JsonPathQuery](../J/JsonPathQuery.md)
  - [JsonPathValue](../J/JsonPathValue.md)
  - [JsonTableResetRowPattern](../J/JsonTableResetRowPattern.md)

## Notes and Other Information
- This is a static function serving as the primary JSONPath execution interface
- Handles both existence checks (when result is NULL) and value collection modes
- Implements performance optimization for existence-only queries in strict mode
- Manages execution context including lax/strict modes, error policies, and timezone settings
- Critical for all JSONPath functionality in PostgreSQL's JSON processing
- Located in src/backend/utils/adt/jsonpath_exec.c:679-734
- Maintains pointers into input values, requiring them to remain available during execution
- Supports variable substitution through callback mechanisms for dynamic JSONPath expressions

## Simplified Source

```c
static JsonPathExecResult executeJsonPath(JsonPath *path, void *vars,
                                         JsonPathGetVarCallback getVar,
                                         JsonPathCountVarsCallback countVars,
                                         Jsonb *json, bool throwErrors,
                                         JsonValueList *result, bool useTz) {
    JsonPathExecContext cxt;
    JsonPathExecResult res;
    JsonPathItem jsp;
    JsonbValue jbv;

    // Initialize JSONPath parser
    jspInit(&jsp, path);

    // Extract root value from JSONB document
    if (!JsonbExtractScalar(&json->root, &jbv)) {
        JsonbInitBinary(&jbv, json);
    }

    // Set up execution context
    cxt.vars = vars;
    cxt.getVar = getVar;
    cxt.laxMode = (path->header & JSONPATH_LAX) != 0;
    cxt.ignoreStructuralErrors = cxt.laxMode;
    cxt.root = &jbv;
    cxt.current = &jbv;
    cxt.baseObject.jbc = NULL;
    cxt.baseObject.id = 0;
    cxt.lastGeneratedObjectId = 1 + countVars(vars);
    cxt.innermostArraySize = -1;
    cxt.throwErrors = throwErrors;
    cxt.useTz = useTz;

    // Optimization for strict mode existence checks
    if (jspStrictAbsenceOfErrors(&cxt) && !result) {
        JsonValueList vals = {0};
        res = executeItem(&cxt, &jsp, &jbv, &vals);

        if (jperIsError(res))
            return res;

        return JsonValueListIsEmpty(&vals) ? jperNotFound : jperOk;
    }

    // Execute JSONPath expression
    res = executeItem(&cxt, &jsp, &jbv, result);

    Assert(!throwErrors || !jperIsError(res));

    return res;
}
```