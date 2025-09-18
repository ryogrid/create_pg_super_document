# executeNestedBoolItem

## Location
[src/backend/utils/adt/jsonpath_exec.c:1913-1933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L1913-L1933)

## Overview
Executes nested boolean expressions (such as filters) in JSON path processing by pushing the current SQL/JSON item onto the execution stack.

## Definition


## Detailed Description
This function is a specialized wrapper for executing boolean expressions within nested contexts during JSON path evaluation. It temporarily modifies the execution context by setting the current item to the provided JsonbValue, executes the boolean item, and then restores the previous current item. This stack-like behavior is essential for maintaining proper context when evaluating nested expressions, particularly in filter operations where the evaluation context needs to be adjusted temporarily.

The function ensures that nested boolean evaluations operate on the correct JSON item while preserving the outer execution context. It's a critical component in the JSON path execution engine that handles scope management for nested expressions.

## Parameters / Member Variables
- : Pointer to the JSON path execution context containing the current evaluation state
- : Pointer to the JSON path item representing the boolean expression to execute
- : Pointer to the JsonbValue that should become the current item during nested execution

## Dependencies
- Functions called/Symbols referenced:
  - [executeBoolItem](executeBoolItem.md)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md)
  - Used in error handling contexts (RETURN_ERROR)

## Notes and Other Information
- This is a static function, indicating it's only used within the jsonpath_exec.c compilation unit
- The function maintains execution context integrity by properly saving and restoring the previous current item
- Essential for filter operations and other nested boolean expressions in JSON path queries
- Part of the JSON path execution engine that implements SQL/JSON path functionality