# executeJsonPath

## Location
src/backend/utils/adt/jsonpath_exec.c: 679 - 734

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
  - JsonPathExecContext (execution context structure)
  - JsonPathExecResult (return type enumeration)
  - JsonPathItem (path parsing structure)
  - JsonbValue (JSONB value representation)
  - jspInit (JSONPath parser initialization)
  - JsonbExtractScalar (JSONB scalar extraction)
  - JsonbInitBinary (JSONB binary initialization)
  - jspStrictAbsenceOfErrors (strict mode checking)
  - executeItem (recursive item execution)
  - jperIsError (error result checking)
  - JsonValueListIsEmpty (result list checking)
- Called from (representative examples):
  - jsonb_path_exists_internal
  - jsonb_path_query_internal
  - jsonb_path_query_first_internal
  - jsonb_path_query_array_internal
  - JsonPathExists
  - JsonPathQuery
  - JsonPathValue
  - JsonTableResetRowPattern

## Notes and Other Information
- This is a static function serving as the primary JSONPath execution interface
- Handles both existence checks (when result is NULL) and value collection modes
- Implements performance optimization for existence-only queries in strict mode
- Manages execution context including lax/strict modes, error policies, and timezone settings
- Critical for all JSONPath functionality in PostgreSQL's JSON processing
- Located in src/backend/utils/adt/jsonpath_exec.c:679-734
- Maintains pointers into input values, requiring them to remain available during execution
- Supports variable substitution through callback mechanisms for dynamic JSONPath expressions