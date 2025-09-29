# check_functions_in_node

## Location
[src/backend/nodes/nodeFuncs.c:1900-2082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L1900-L2082)

## Overview
Applies a checker function to each function OID contained in a given expression node to determine if any contained functions meet specific criteria.

## Definition

```c
struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr	   *expr = (OpExpr *) node;

				/* Set opfuncid if it wasn't set already */
				set_opfuncid(expr);
				if (checker(expr->opfuncid, context))
					return true;
			}
			break;
```
## Detailed Description
This function examines a single expression node and applies a checker callback function to every SQL function OID contained within that node. It returns true if the checker function returns true for any of the contained functions, and false if the node contains no SQL-visible functions or if the checker returns false for all functions.

The function handles various expression node types including Aggref, WindowFunc, FuncExpr, OpExpr, DistinctExpr, NullIfExpr, ScalarArrayOpExpr, CoerceViaIO, and RowCompareExpr. For operator expressions, it ensures that opfuncid is set before checking. For CoerceViaIO, it checks both input and output functions. For RowCompareExpr, it checks all comparison operators.

Important: This function does NOT recurse into sub-expressions; it only examines the given node. Callers are responsible for controlling recursion through the expression tree.

## Parameters / Member Variables
- `node`: The expression node to examine (must not be NULL)
- `set_opfuncid(expr)`: Callback function that takes a function OID and context, returns bool
- `true`: Arbitrary context data passed to the checker function
## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - [set_opfuncid](../s/set_opfuncid.md)
  - [set_sa_opfuncid](../s/set_sa_opfuncid.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [get_opcode](../g/get_opcode.md)
  - [exprType](../e/exprType.md)
  - lfirst_oid
- Called from (representative examples):
  - [check_simple_rowfilter_expr_walker](check_simple_rowfilter_expr_walker.md)
  - [contain_mutable_functions_walker](contain_mutable_functions_walker.md)
  - [contain_volatile_functions_walker](contain_volatile_functions_walker.md)
  - [contain_volatile_functions_not_nextval_walker](contain_volatile_functions_not_nextval_walker.md)
  - [max_parallel_hazard_walker](../m/max_parallel_hazard_walker.md)
  - [contain_nonstrict_functions_walker](contain_nonstrict_functions_walker.md)
  - [contain_leaked_vars_walker](contain_leaked_vars_walker.md)

## Notes and Other Information
- Does not recurse into sub-expressions - caller controls recursion
- Ignores MinMaxExpr, SQLValueFunction, XmlExpr, CoerceToDomain, and NextValueExpr nodes
- Sets operator function IDs if not already set before checking
- For CoerceViaIO, checks both source type's output function and target type's input function
- Essential for analyzing function properties like volatility, mutability, and parallel safety across expression trees

## Simplified Source

```c
bool check_functions_in_node(Node *node, check_function_callback checker, void *context) {
    // Apply checker to function OID based on node type
    switch (nodeTag(node)) {
        case T_Aggref:
            // Check aggregate function
            return checker(((Aggref *) node)->aggfnoid, context);

        case T_WindowFunc:
            // Check window function
            return checker(((WindowFunc *) node)->winfnoid, context);

        case T_FuncExpr:
            // Check regular function
            return checker(((FuncExpr *) node)->funcid, context);

        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
            // Check operator function (ensure opfuncid is set)
            OpExpr *expr = (OpExpr *) node;
            set_opfuncid(expr);
            return checker(expr->opfuncid, context);

        case T_ScalarArrayOpExpr:
            // Check scalar array operator function
            ScalarArrayOpExpr *sa_expr = (ScalarArrayOpExpr *) node;
            set_sa_opfuncid(sa_expr);
            return checker(sa_expr->opfuncid, context);

        case T_CoerceViaIO:
            // Check both input and output functions for type conversion
            CoerceViaIO *coerce_expr = (CoerceViaIO *) node;
            Oid input_func, output_func, param;
            bool is_varlena;

            getTypeInputInfo(coerce_expr->resulttype, &input_func, &param);
            if (checker(input_func, context))
                return true;

            getTypeOutputInfo(exprType((Node *) coerce_expr->arg), &output_func, &is_varlena);
            return checker(output_func, context);

        case T_RowCompareExpr:
            // Check all comparison operators in row comparison
            RowCompareExpr *row_expr = (RowCompareExpr *) node;
            ListCell *op_cell;

            foreach(op_cell, row_expr->opnos) {
                Oid op_func = get_opcode(lfirst_oid(op_cell));
                if (checker(op_func, context))
                    return true;
            }
            break;
    }

    return false;
}
```