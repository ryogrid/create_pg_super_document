# dependency_is_compatible_clause

## Location
[src/backend/statistics/dependencies.c:741-928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L741-L928)

## Overview
Determines if a clause is compatible with functional dependencies by analyzing whether it represents an equality condition with a pseudoconstant that can be used for dependency-based selectivity estimation.

## Definition

```c
static bool
dependency_is_compatible_clause(Node *clause, Index relid, AttrNumber *attnum)
```
## Detailed Description
This function examines a WHERE clause to determine if it's suitable for use with functional dependencies in selectivity estimation. The function accepts clauses that have the form of equality to a pseudoconstant, or can be interpreted that way. The variable part of the clause must be a simple Var belonging to the specified relation.

The function handles several types of clauses:
- **OpExpr**: Checks for  or  patterns using equality operators
- **ScalarArrayOpExpr**: Handles  expressions with ANY semantics
- **OR clauses**: Recursively processes OR expressions ensuring all sub-clauses reference the same attribute
- **NOT clauses**: Interprets  as 
- **Boolean expressions**: Interprets bare boolean  as 

The function validates that the operator used is an equality operator by checking if  returns , ensuring compatibility with functional dependency logic.

## Parameters / Member Variables
- `*clause`: The clause node to examine for compatibility with functional dependencies
- `relid`: The relation index that the clause should reference
- `*attnum`: Output parameter that receives the attribute number of the variable on success
## Dependencies
- Functions called/Symbols referenced:
  - [bms_membership](../b/bms_membership.md)
  - [is_opclause](../i/is_opclause.md)
  - [is_pseudo_constant_clause](../i/is_pseudo_constant_clause.md)
  - [get_oprrest](../g/get_oprrest.md)
  - [is_orclause](../i/is_orclause.md)
  - [is_notclause](../i/is_notclause.md)
  - [get_notclausearg](../g/get_notclausearg.md)
  - AttrNumberIsForUserDefinedAttr
- Called from (representative examples):
  - DependencyGenerator
  - [dependency_is_compatible_clause](dependency_is_compatible_clause.md) (recursive call for OR clauses)
  - [dependencies_clauselist_selectivity](dependencies_clauselist_selectivity.md)

## Notes and Other Information
- Only supports simple Var expressions, not complex expressions or functions
- Rejects pseudoconstant clauses since they cannot contain variables
- Ensures clauses reference only a single relation (singleton bitmap membership)
- Filters out system attributes as statistics are not maintained for them
- Uses a somewhat dubious method of checking equality operators via selectivity functions rather than btree/hash opclass membership
- The function is recursive when processing OR clauses to ensure all sub-clauses reference the same attribute

## Simplified Source

```c
static bool
dependency_is_compatible_clause(Node *clause, Index relid, AttrNumber *attnum)
{
    Var *var;
    Node *clause_expr;

    // Handle RestrictInfo wrapper
    if (IsA(clause, RestrictInfo)) {
        RestrictInfo *rinfo = (RestrictInfo *) clause;

        // Skip pseudoconstants and multi-relation clauses
        if (rinfo->pseudoconstant ||
            bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
            return false;

        clause = (Node *) rinfo->clause;
    }

    // Handle different clause types
    if (is_opclause(clause)) {
        // Process Var = Const or Const = Var
        OpExpr *expr = (OpExpr *) clause;

        if (list_length(expr->args) != 2)
            return false;

        // Determine which argument is variable vs constant
        if (is_pseudo_constant_clause(lsecond(expr->args)))
            clause_expr = linitial(expr->args);
        else if (is_pseudo_constant_clause(linitial(expr->args)))
            clause_expr = lsecond(expr->args);
        else
            return false;

        // Only accept equality operators
        if (get_oprrest(expr->opno) != F_EQSEL)
            return false;

    } else if (IsA(clause, ScalarArrayOpExpr)) {
        // Process Var IN Const (ANY semantics only)
        ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) clause;

        if (!expr->useOr || list_length(expr->args) != 2)
            return false;

        if (!is_pseudo_constant_clause(lsecond(expr->args)))
            return false;

        clause_expr = linitial(expr->args);

        // Only accept equality operators
        if (get_oprrest(expr->opno) != F_EQSEL)
            return false;

    } else if (is_orclause(clause)) {
        // Recursively process OR clauses
        BoolExpr *bool_expr = (BoolExpr *) clause;

        *attnum = InvalidAttrNumber;
        foreach(lc, bool_expr->args) {
            AttrNumber clause_attnum;

            // All sub-clauses must be compatible
            if (!dependency_is_compatible_clause((Node *) lfirst(lc), relid, &clause_attnum))
                return false;

            // All sub-clauses must reference same attribute
            if (*attnum == InvalidAttrNumber)
                *attnum = clause_attnum;
            else if (*attnum != clause_attnum)
                return false;
        }
        return true;

    } else if (is_notclause(clause)) {
        // Handle NOT x as x = false
        clause_expr = (Node *) get_notclausearg(clause);
    } else {
        // Handle boolean x as x = true
        clause_expr = (Node *) clause;
    }

    // Strip RelabelType if present
    if (IsA(clause_expr, RelabelType))
        clause_expr = (Node *) ((RelabelType *) clause_expr)->arg;

    // Only support simple Vars
    if (!IsA(clause_expr, Var))
        return false;

    var = (Var *) clause_expr;

    // Validate Var properties
    if (var->varno != relid || var->varlevelsup != 0 ||
        !AttrNumberIsForUserDefinedAttr(var->varattno))
        return false;

    *attnum = var->varattno;
    return true;
}
```