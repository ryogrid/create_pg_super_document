# copyJsonbValue

## Location
src/backend/utils/adt/jsonpath_exec.c: 3445 - 3458

## Overview
Creates a shallow copy of a JsonbValue structure by allocating new memory and copying the structure contents.

## Definition


## Detailed Description
The copyJsonbValue function creates a shallow copy of a JsonbValue structure. It allocates memory for a new JsonbValue using palloc() and performs a structure assignment to copy all fields from the source to the destination. This is a utility function used internally in JSONPath execution to create copies of JSON values when needed.

## Parameters / Member Variables
- : Pointer to the source JsonbValue structure to be copied

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [JsonbValue](../J/JsonbValue.md) (structure type)
- Called from (representative examples):
  - [executeNextItem](../e/executeNextItem.md)
  - [executeAnyItem](../e/executeAnyItem.md)
  - RETURN_ERROR macro

## Notes and Other Information
- This performs a shallow copy - if the JsonbValue contains pointers to other structures, only the pointer values are copied, not the referenced data
- Memory is allocated using palloc(), which is PostgreSQL's memory management system
- The function is static, meaning it's only visible within the jsonpath_exec.c file
- Used in JSONPath execution error handling and item processing contexts