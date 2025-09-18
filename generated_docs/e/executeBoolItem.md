# executeBoolItem

## Location
src/backend/utils/adt/jsonpath_exec.c: 1777 - 1912

## Overview
This function executes boolean-valued JSONPath expressions, handling logical operations, comparisons, and existence checks with proper three-valued logic support.

## Definition


## Detailed Description
The  function is the core boolean expression evaluator for JSONPath. It implements a comprehensive set of boolean operations including logical AND/OR/NOT, comparison operations, pattern matching, and existence checks. The function uses three-valued logic (true/false/unknown) to handle SQL/JSON semantics correctly. It processes various JSONPath item types recursively, implementing proper short-circuit evaluation for logical operations and delegating predicate evaluation to specialized functions. The function includes stack depth checking to prevent overflow during recursive evaluation.

## Parameters / Member Variables
- : JSONPath execution context containing state and configuration
- : JSONPath item representing the boolean expression to evaluate
- : JsonbValue containing the input data to evaluate against
- : Boolean flag indicating whether the item can have subsequent items

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - jspHasNext
  - jspGetLeftArg
  - jspGetRightArg  
  - jspGetArg
  - jspInitByBuffer
  - jspStrictAbsenceOfErrors
  - executeBoolItem (recursive)
  - executePredicate
  - executeComparison
  - executeStartsWith
  - executeLikeRegex
  - executeItemOptUnwrapResultNoThrow
  - jperIsError
  - JsonValueListIsEmpty
  - JsonPathBool (return type)
  - JsonPathItem (type)
  - JsonValueList (type)
  - JsonLikeRegexContext (type)
  - Various enum values (jpiAnd, jpiOr, jpiNot, jpiEqual, etc.)
  - Three-valued logic constants (jpbTrue, jpbFalse, jpbUnknown)
- Called from (representative examples):
  - executeItemOptUnwrapTarget
  - executeBoolItem (recursive calls)
  - executeNestedBoolItem

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c
- Implements proper SQL/JSON three-valued logic semantics where operations can return true, false, or unknown
- Includes recursive calls with stack depth protection to handle nested boolean expressions
- Handles short-circuit evaluation for AND/OR operations while respecting SQL/JSON error handling requirements
- Supports comparison operations (=, !=, <, >, <=, >=), string operations (STARTS WITH, LIKE_REGEX), and existence checks
- The jpiExists case has different behavior in strict vs lax modes, collecting all values in strict mode to ensure no errors occur
- Uses specialized predicate execution functions for complex operations like regex matching and comparisons
- Error conditions result in jpbUnknown return values, maintaining three-valued logic consistency
- Critical for implementing JSONPath filter expressions and boolean predicates in PostgreSQL's JSON functionality