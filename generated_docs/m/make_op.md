# make_op

## Location
[src/backend/parser/parse_oper.c:660-769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L660-L769)

## Overview
The  function constructs operator expressions in PostgreSQL's parser, handling type compatibility, operator resolution, and building the final expression tree.

## Definition

```c
Expr *
make_op(ParseState *pstate, List *opname, Node *ltree, Node *rtree,
		Node *last_srf, int location)
```
## Detailed Description
This function is the primary entry point for constructing operator expressions during parsing. It handles both unary (prefix) and binary operators by analyzing the provided operand nodes and resolving the appropriate operator from the system catalogs. The function performs comprehensive type checking, ensures operator compatibility, handles type coercion when necessary, and constructs the final OpExpr node.

The function distinguishes between prefix operators (when ltree is NULL) and binary operators, calling the appropriate resolution functions (left_oper or oper). It validates that the operator is not a shell operator and performs polymorphic type resolution to ensure type consistency. The function also handles set-returning function validation and placement checking.

## Parameters / Member Variables
- : ParseState for context and error reporting
- : List containing the operator name components
- : Left operand expression node (NULL for prefix operators)
- : Right operand expression node (required)
- : Copy of pstate->p_last_srf for nested set-returning function detection
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [exprType](../e/exprType.md)
  - [left_oper](../l/left_oper.md)
  - [oper](../o/oper.md)
  - [op_signature_string](../o/op_signature_string.md)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md)
  - [make_fn_arguments](make_fn_arguments.md)
  - makeNode
  - [oprid](../o/oprid.md)
  - [get_func_retset](../g/get_func_retset.md)
  - [check_srf_call_placement](../c/check_srf_call_placement.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [transformAExprOp](../t/transformAExprOp.md)
  - [transformAExprNullIf](../t/transformAExprNullIf.md)
  - [transformAExprIn](../t/transformAExprIn.md)
  - [make_row_comparison_op](make_row_comparison_op.md)
  - [make_distinct_op](make_distinct_op.md)

## Notes and Other Information
- Returns an Expr node (specifically an OpExpr) representing the operator expression
- Does not support postfix operators - will raise an error if rtree is NULL
- Validates that operators are not shell operators (incomplete operator definitions)
- Handles polymorphic type resolution for generic operators
- Performs automatic type coercion when necessary through make_fn_arguments
- Tracks set-returning functions for proper placement validation
- The opcollid and inputcollid fields are set later by parse_collate.c
- Must release the syscache entry for the operator tuple when done

## Simplified Source

```c
Expr *
make_op(ParseState *pstate, List *opname, Node *ltree, Node *rtree,
        Node *last_srf, int location)
{
    Oid ltypeId, rtypeId;
    Operator tup;
    Form_pg_operator opform;
    Oid actual_arg_types[2];
    Oid declared_arg_types[2];
    int nargs;
    List *args;
    Oid rettype;
    OpExpr *result;

    // Reject postfix operators
    if (rtree == NULL)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("postfix operators are not supported")));

    // Resolve operator based on argument types
    if (ltree == NULL)
    {
        // Prefix operator
        rtypeId = exprType(rtree);
        ltypeId = InvalidOid;
        tup = left_oper(pstate, opname, rtypeId, false, location);
        args = list_make1(rtree);
        actual_arg_types[0] = rtypeId;
        nargs = 1;
    }
    else
    {
        // Binary operator
        ltypeId = exprType(ltree);
        rtypeId = exprType(rtree);
        tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
        args = list_make2(ltree, rtree);
        actual_arg_types[0] = ltypeId;
        actual_arg_types[1] = rtypeId;
        nargs = 2;
    }

    opform = (Form_pg_operator) GETSTRUCT(tup);

    // Validate operator is not a shell
    if (!RegProcedureIsValid(opform->oprcode))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                       errmsg("operator is only a shell: %s",
                             op_signature_string(opname, opform->oprleft, opform->oprright)),
                       parser_errposition(pstate, location)));

    // Set up declared argument types
    if (ltree == NULL)
    {
        declared_arg_types[0] = opform->oprright;
    }
    else
    {
        declared_arg_types[0] = opform->oprleft;
        declared_arg_types[1] = opform->oprright;
    }

    // Handle polymorphic types
    rettype = enforce_generic_type_consistency(actual_arg_types, declared_arg_types,
                                              nargs, opform->oprresult, false);

    // Perform necessary type coercions
    make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types);

    // Build the OpExpr node
    result = makeNode(OpExpr);
    result->opno = oprid(tup);
    result->opfuncid = opform->oprcode;
    result->opresulttype = rettype;
    result->opretset = get_func_retset(opform->oprcode);
    result->args = args;
    result->location = location;

    // Handle set-returning functions
    if (result->opretset)
    {
        check_srf_call_placement(pstate, last_srf, location);
        pstate->p_last_srf = (Node *) result;
    }

    ReleaseSysCache(tup);
    return (Expr *) result;
}
```