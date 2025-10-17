# arraycontsel

## Location
[src/backend/utils/adt/array_selfuncs.c:241-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L241-L320)

## Overview
Restriction selectivity function for array containment operators (@>, &&, <@), estimating the probability that array containment conditions will be satisfied.

## Definition

```c
Datum
arraycontsel(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the main entry point for selectivity estimation of array containment operations in PostgreSQL's query planner. It handles three types of array operators:
-  (contains): estimates probability that left array contains right array
-  (contained by): estimates probability that left array is contained by right array  
-  (overlaps): estimates probability that arrays have common elements

The function validates that the expression follows the pattern '(variable op constant)' or '(constant op variable)', then delegates the actual calculation to calc_arraycontsel(). It handles operator commutation when the variable is on the right side and validates type compatibility between the operands.

## Parameters
Uses PostgreSQL's standard function argument interface:
- : PlannerInfo pointer (root)
- : Operator OID 
- : List of arguments
- : Variable relation ID (varRelid)

## Dependencies
- Functions called/Symbols referenced:
  - [get_restriction_variable](../g/get_restriction_variable.md)
  - ReleaseVariableStats  
  - DEFAULT_SEL
  - [get_base_element_type](../g/get_base_element_type.md)
  - [calc_arraycontsel](../c/calc_arraycontsel.md)
  - CLAMP_PROBABILITY
- Called from:
  - (No direct references found - likely registered as operator selectivity function)

## Notes and Other Information
- Returns default selectivity estimate if expression doesn't match expected pattern
- Handles NULL constants by returning 0.0 selectivity (since operators are strict)
- Validates element type compatibility between variable and constant array
- Uses PostgreSQL's function call interface (PG_FUNCTION_ARGS/PG_RETURN_FLOAT8)
- Part of PostgreSQL's extensible operator selectivity framework

## Simplified Source

```c
Datum arraycontsel(PG_FUNCTION_ARGS) {
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid operator = PG_GETARG_OID(1);
    List *args = (List *) PG_GETARG_POINTER(2);
    int varRelid = PG_GETARG_INT32(3);

    VariableStatData vardata;
    Node *other;
    bool varonleft;

    // Check if expression is (variable op constant) or (constant op variable)
    if (!get_restriction_variable(root, args, varRelid, &vardata, &other, &varonleft))
        PG_RETURN_FLOAT8(DEFAULT_SEL(operator));

    // Must have a constant operand
    if (!IsA(other, Const)) {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(DEFAULT_SEL(operator));
    }

    // NULL constant always returns 0.0 (operators are strict)
    if (((Const *) other)->constisnull) {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(0.0);
    }

    // Commute operator if variable is on the right
    if (!varonleft) {
        if (operator == OID_ARRAY_CONTAINS_OP)
            operator = OID_ARRAY_CONTAINED_OP;
        else if (operator == OID_ARRAY_CONTAINED_OP)
            operator = OID_ARRAY_CONTAINS_OP;
    }

    SelectivityValue selec;

    // Check element type compatibility and calculate selectivity
    Oid element_typeid = get_base_element_type(((Const *) other)->consttype);
    if (element_typeid != InvalidOid &&
        element_typeid == get_base_element_type(vardata.vartype)) {
        selec = calc_arraycontsel(&vardata, ((Const *) other)->constvalue,
                                  element_typeid, operator);
    } else {
        selec = DEFAULT_SEL(operator);
    }

    ReleaseVariableStats(vardata);
    CLAMP_PROBABILITY(selec);
    PG_RETURN_FLOAT8((float8) selec);
}
```