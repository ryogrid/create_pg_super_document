# ParseComplexProjection

## Location
src/backend/parser/parse_func.c: 1912 - 1992

## Overview
Handles function calls with a single complex-type argument by checking if the call is actually a column projection and transforming it into the appropriate expression tree.

## Definition


## Detailed Description
The  function is a crucial component of PostgreSQL's function call resolution mechanism that handles the special case where what appears to be a function call is actually a column projection from a complex type (composite type or record). When parsing expressions like , this function determines whether  is actually a field/column of the complex type represented by , rather than a function call.

The function employs two different strategies depending on the nature of the first argument:

1. **Whole-row Var optimization**: For whole-row variables (varattno == InvalidAttrNumber), it uses a more efficient direct lookup approach that can resolve projections like  even when  references a subselect, join, or RECORD function.

2. **General complex type handling**: For other cases, it uses  to obtain the tuple descriptor of the expression, with special handling for RECORD types via .

If the funcname matches a valid, non-dropped column in the tuple descriptor, the function generates a  expression node that represents the column projection.

## Parameters / Member Variables
- : Parse state context for the current parsing operation
- : The name being looked up as a potential column/field name
- : The complex-type expression that might contain the named field
- : Source location information for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - GetNSItemByRangeTablePosn
  - scanNSItemForColumn
  - expandRecordVariable
  - get_expr_result_tupdesc
  - TupleDescAttr
  - makeNode
  - FieldSelect (struct)
  - ParseNamespaceItem (struct)
  - InvalidAttrNumber (constant)
- Called from (representative examples):
  - ParseFuncOrColumn
  - FuncLookupError

## Notes and Other Information
- Returns  if the funcname does not correspond to a valid field/column in the complex type
- The function is static, limiting its scope to parse_func.c
- Provides an optimization for whole-row Vars that avoids generating unnecessary FieldSelect nodes when possible
- Handles both regular composite types and special RECORD types that require variable expansion
- The generated FieldSelect expression includes proper type information (resulttype, resulttypmod) and collation details for downstream processing
- Field numbers in PostgreSQL are 1-based, so the function adds 1 to the array index when setting 