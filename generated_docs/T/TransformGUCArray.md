# TransformGUCArray

## Location
[src/backend/utils/misc/guc.c:6407-6463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6407-L6463)

## Overview
TransformGUCArray converts a PostgreSQL array of GUC settings into separate lists of parameter names and values, optimized for scenarios where the settings need to be applied repeatedly.

## Definition

```c
void
TransformGUCArray(ArrayType *array, List **names, List **values)
```
## Detailed Description
TransformGUCArray processes a PostgreSQL ArrayType containing text elements that represent GUC settings in "name=value" format. The function transforms this array into two separate linked lists (names and values) that can be processed more efficiently when settings need to be applied multiple times, such as during function invocations.

The function performs these operations:
1. **Validate input**: Ensures the array is one-dimensional, contains TEXT elements, and uses 1-based indexing
2. **Extract elements**: Iterates through each array element using array_ref
3. **Parse settings**: Uses ParseLongOption to separate each "name=value" string
4. **Build lists**: Constructs parallel lists of names and values using lappend
5. **Error handling**: Issues warnings for malformed settings (missing '=' delimiter)

The resulting lists maintain correspondence by position, making it easy to apply settings by iterating both lists simultaneously.

## Parameters / Member Variables
- : Input ArrayType containing GUC settings as text elements in "name=value" format
- : Output parameter receiving a List of parameter names (char* strings)
- : Output parameter receiving a List of parameter values (char* strings)

## Dependencies
- Functions called/Symbols referenced:
  - ARR_ELEMTYPE/ARR_NDIM/ARR_LBOUND/ARR_DIMS (array metadata macros)
  - array_ref (extract individual array elements)
  - TextDatumGetCString (convert text datum to C string)
  - ParseLongOption (parse "name=value" strings)
  - lappend (append to linked lists)
  - pfree (free allocated memory)
- Called from (representative examples):
  - fmgr_security_definer (src/backend/utils/fmgr/fmgr.c:679)
  - ProcessGUCArray (src/backend/utils/misc/guc.c:6472)

## Notes and Other Information
- The function expects arrays with 1-based lower bounds (ARR_LBOUND(array)[0] == 1)
- Memory for names and values is allocated using palloc and should be freed by caller
- Null array elements are silently skipped without generating warnings
- Settings without '=' delimiter generate warnings but don't cause failures
- The output lists are initialized to NIL and built incrementally
- Designed for performance optimization in contexts like security definer functions where GUC arrays are processed repeatedly