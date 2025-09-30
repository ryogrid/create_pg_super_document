# make_scalar_array_op

## Location
[src/backend/parser/parse_oper.c:770-936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L770-L936)

## Overview
The  function builds expression trees for "scalar op ANY/ALL (array)" constructs in PostgreSQL's parser, handling type resolution and validation for array operations.

## Definition

```c
struct given operator name and arg types.
 *
 * Returns true if successful, false if the search_path overflowed
 * (hence no caching is possible).
 *
 * pstate/location are used only to report the error position;
```
## Detailed Description
This function constructs ScalarArrayOpExpr nodes for SQL constructs like "value = ANY(array)" or "value <> ALL(array)". It performs comprehensive type checking to ensure the right-hand side is an array type and extracts the element type for operator resolution. The function validates that the operator returns a boolean result and doesn't return a set, as required for array operations.

The function handles polymorphic operators carefully, ensuring type consistency between the scalar value, array elements, and operator requirements. It also manages type coercion as needed and constructs the final expression node with appropriate operator and function identifiers.

## Parameters / Member Variables
- : ParseState for context and error reporting
- : List containing the operator name components
- : Boolean flag indicating ANY (true) vs ALL (false) semantics
- : Left operand expression node (the scalar value)
- : Right operand expression node (the array)
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [exprType](../e/exprType.md)
  - [get_base_element_type](../g/get_base_element_type.md)
  - [oper](../o/oper.md)
  - [op_signature_string](../o/op_signature_string.md)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md)
  - [get_func_retset](../g/get_func_retset.md)
  - IsPolymorphicType
  - [get_array_type](../g/get_array_type.md)
  - [make_fn_arguments](make_fn_arguments.md)
  - makeNode
  - [oprid](../o/oprid.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [transformAExprOpAny](../t/transformAExprOpAny.md)
  - [transformAExprOpAll](../t/transformAExprOpAll.md)
  - [transformAExprIn](../t/transformAExprIn.md)

## Notes and Other Information
- Returns a ScalarArrayOpExpr node representing the scalar-array operation
- Requires the right-hand side to be an array type, raising an error otherwise
- Validates that the operator returns boolean and doesn't return a set
- Handles polymorphic operators by adjusting array types as needed
- The useOr parameter determines ANY vs ALL semantics for the operation
- The inputcollid field is set later by parse_collate.c
- Uses UNKNOWNOID handling for untyped literals on the right-hand side
- Performs automatic type coercion through make_fn_arguments when necessary
- The hashfuncid and negfuncid fields are initialized to InvalidOid and may be set later for optimization

## Simplified Source

```c
Expr *
make_scalar_array_op(ParseState *pstate, List *opname, bool useOr,
                     Node *ltree, Node *rtree, int location) {
    Oid ltypeId, rtypeId, atypeId, res_atypeId;
    Operator tup;
    Form_pg_operator opform;

    // Get types from left and right expressions
    ltypeId = exprType(ltree);
    atypeId = exprType(rtree);

    // Extract element type from array (right side)
    if (atypeId == UNKNOWNOID) {
        rtypeId = UNKNOWNOID;  // Handle untyped literals
    } else {
        rtypeId = get_base_element_type(atypeId);
        if (!OidIsValid(rtypeId)) {
            ereport(ERROR, "op ANY/ALL requires array on right side");
        }
    }

    // Find the operator
    tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
    opform = (Form_pg_operator) GETSTRUCT(tup);

    // Validate operator is not just a shell
    if (!RegProcedureIsValid(opform->oprcode)) {
        ereport(ERROR, "operator is only a shell");
    }

    // Build argument list and type arrays
    List *args = list_make2(ltree, rtree);
    Oid actual_arg_types[2] = {ltypeId, rtypeId};
    Oid declared_arg_types[2] = {opform->oprleft, opform->oprright};

    // Ensure type consistency for polymorphic operators
    Oid rettype = enforce_generic_type_consistency(actual_arg_types,
                                                  declared_arg_types, 2,
                                                  opform->oprresult, false);

    // Validate operator returns boolean and not a set
    if (rettype != BOOLOID) {
        ereport(ERROR, "op ANY/ALL requires operator to yield boolean");
    }
    if (get_func_retset(opform->oprcode)) {
        ereport(ERROR, "op ANY/ALL requires operator not to return a set");
    }

    // Handle array type for polymorphic operators
    if (IsPolymorphicType(declared_arg_types[1])) {
        res_atypeId = atypeId;  // Use actual array type
    } else {
        res_atypeId = get_array_type(declared_arg_types[1]);
        if (!OidIsValid(res_atypeId)) {
            ereport(ERROR, "could not find array type");
        }
    }

    // Apply necessary type casts
    actual_arg_types[1] = atypeId;
    declared_arg_types[1] = res_atypeId;
    make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types);

    // Create the final expression node
    ScalarArrayOpExpr *result = makeNode(ScalarArrayOpExpr);
    result->opno = oprid(tup);
    result->opfuncid = opform->oprcode;
    result->hashfuncid = InvalidOid;
    result->negfuncid = InvalidOid;
    result->useOr = useOr;
    result->args = args;
    result->location = location;

    ReleaseSysCache(tup);
    return (Expr *) result;
}
```