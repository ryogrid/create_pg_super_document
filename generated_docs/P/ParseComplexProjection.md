# ParseComplexProjection

## Location
[src/backend/parser/parse_func.c:1912-1992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L1912-L1992)

## Overview
Handles function calls with a single complex-type argument by checking if the call is actually a column projection and transforming it into the appropriate expression tree.

## Definition

```c
static Node *
ParseComplexProjection(ParseState *pstate, const char *funcname, Node *first_arg,
					   int location)
```
## Detailed Description
The  function is a crucial component of PostgreSQL's function call resolution mechanism that handles the special case where what appears to be a function call is actually a column projection from a complex type (composite type or record). When parsing expressions like , this function determines whether  is actually a field/column of the complex type represented by , rather than a function call.

The function employs two different strategies depending on the nature of the first argument:

1. **Whole-row Var optimization**: For whole-row variables (varattno == InvalidAttrNumber), it uses a more efficient direct lookup approach that can resolve projections like  even when  references a subselect, join, or RECORD function.

2. **General complex type handling**: For other cases, it uses  to obtain the tuple descriptor of the expression, with special handling for RECORD types via .

If the funcname matches a valid, non-dropped column in the tuple descriptor, the function generates a  expression node that represents the column projection.

## Parameters / Member Variables
- `*pstate`: Parse state context for the current parsing operation
- `*funcname`: The name being looked up as a potential column/field name
- `*first_arg`: The complex-type expression that might contain the named field
- `location`: Source location information for error reporting
## Dependencies
- Functions called/Symbols referenced:
  - [GetNSItemByRangeTablePosn](../G/GetNSItemByRangeTablePosn.md)
  - [scanNSItemForColumn](../s/scanNSItemForColumn.md)
  - [expandRecordVariable](../e/expandRecordVariable.md)
  - [get_expr_result_tupdesc](../g/get_expr_result_tupdesc.md)
  - TupleDescAttr
  - makeNode
  - [FieldSelect](../F/FieldSelect.md) (struct)
  - [ParseNamespaceItem](ParseNamespaceItem.md) (struct)
  - InvalidAttrNumber (constant)
- Called from (representative examples):
  - [ParseFuncOrColumn](ParseFuncOrColumn.md)
  - FuncLookupError

## Notes and Other Information
- Returns  if the funcname does not correspond to a valid field/column in the complex type
- The function is static, limiting its scope to parse_func.c
- Provides an optimization for whole-row Vars that avoids generating unnecessary FieldSelect nodes when possible
- Handles both regular composite types and special RECORD types that require variable expansion
- The generated FieldSelect expression includes proper type information (resulttype, resulttypmod) and collation details for downstream processing
- Field numbers in PostgreSQL are 1-based, so the function adds 1 to the array index when setting

## Simplified Source

```c
static Node *ParseComplexProjection(ParseState *pstate, const char *funcname,
                                   Node *first_arg, int location) {
    TupleDesc tupdesc;
    int i;

    // Special optimization for whole-row variables (foo.*)
    if (IsA(first_arg, Var) &&
        ((Var *) first_arg)->varattno == InvalidAttrNumber) {
        ParseNamespaceItem *nsitem;

        nsitem = GetNSItemByRangeTablePosn(pstate,
                                         ((Var *) first_arg)->varno,
                                         ((Var *) first_arg)->varlevelsup);
        // Return a Var if funcname matches a column, else NULL
        return scanNSItemForColumn(pstate, nsitem,
                                 ((Var *) first_arg)->varlevelsup,
                                 funcname, location);
    }

    // Get tuple descriptor for the complex type
    if (IsA(first_arg, Var) && ((Var *) first_arg)->vartype == RECORDOID) {
        // Special handling for RECORD types
        tupdesc = expandRecordVariable(pstate, (Var *) first_arg, 0);
    } else {
        // General case for complex types
        tupdesc = get_expr_result_tupdesc(first_arg, true);
    }

    if (!tupdesc)
        return NULL; // Unresolvable type

    // Search for matching field name in the tuple descriptor
    for (i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        if (strcmp(funcname, NameStr(att->attname)) == 0 && !att->attisdropped) {
            // Found matching field - create FieldSelect expression
            FieldSelect *fselect = makeNode(FieldSelect);

            fselect->arg = (Expr *) first_arg;
            fselect->fieldnum = i + 1; // PostgreSQL uses 1-based field numbers
            fselect->resulttype = att->atttypid;
            fselect->resulttypmod = att->atttypmod;
            fselect->resultcollid = att->attcollation;

            return (Node *) fselect;
        }
    }

    return NULL; // Field name not found
}
``` 