# eqsel_internal

## Location
[src/backend/utils/adt/selfuncs.c:237-295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L237-L295)

## Overview
The eqsel_internal function is the core implementation for selectivity estimation of both equality (=) and inequality (<>) operators, providing the common logic shared between eqsel() and neqsel() functions.

## Definition

```c
static double
eqsel_internal(PG_FUNCTION_ARGS, bool negate)
```
## Detailed Description
The eqsel_internal function performs the actual selectivity estimation calculations for equality and inequality operations. It analyzes the query operator and operands to determine the most appropriate estimation method. The function handles two main scenarios:

1. **Constant comparison**: When one operand is a constant value, it uses var_eq_const for more precise estimation based on histogram data and most common values.
2. **Non-constant comparison**: When both operands are variables or expressions, it uses var_eq_non_const for estimation.

The function also supports negation logic for inequality operators by first computing the equality selectivity and then converting it using the formula: '1.0 - eq_selectivity - nullfrac'. This approach leverages the existing equality estimation infrastructure for inequality operations.

## Parameters / Member Variables
- Standard PostgreSQL function arguments (PG_FUNCTION_ARGS):
  - : PlannerInfo pointer containing query planning context
  - : OID of the comparison operator
  - : List of operator arguments (left and right operands)
  - : Relation ID for variable references
  - : Collation information for the operation
- : Boolean flag indicating whether to compute inequality (true) or equality (false) selectivity

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - [get_negator](../g/get_negator.md)
  - [get_restriction_variable](../g/get_restriction_variable.md)
  - [var_eq_const](../v/var_eq_const.md)
  - [var_eq_non_const](../v/var_eq_non_const.md)
  - ReleaseVariableStats
  - DEFAULT_EQ_SEL (constant)
- Called from (representative examples):
  - [eqsel](eqsel.md)
  - [neqsel](../n/neqsel.md)

## Notes and Other Information
- Located in src/backend/utils/adt/selfuncs.c:237-295
- Returns a double value representing the estimated selectivity (0.0 to 1.0)
- Uses DEFAULT_EQ_SEL as fallback when operator negation fails or when expression structure is not recognized
- Automatically handles collation-aware comparisons
- Releases variable statistics memory using ReleaseVariableStats to prevent memory leaks
- The function's static nature indicates it's an internal implementation detail not exposed to external modules

## Simplified Source

```c
static double
eqsel_internal(PG_FUNCTION_ARGS, bool negate)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid operator = PG_GETARG_OID(1);
    List *args = (List *) PG_GETARG_POINTER(2);
    int varRelid = PG_GETARG_INT32(3);
    Oid collation = PG_GET_COLLATION();
    VariableStatData vardata;
    Node *other;
    bool varonleft;
    double selec;

    // For inequality (<>), use the corresponding equality operator
    // and later convert via "1.0 - eq_selectivity - nullfrac"
    if (negate) {
        operator = get_negator(operator);
        if (!OidIsValid(operator)) {
            return 1.0 - DEFAULT_EQ_SEL;  // Fallback if negator not found
        }
    }

    // Extract variable and comparison operand from expression
    // Return default if not in form "variable = something"
    if (!get_restriction_variable(root, args, varRelid,
                                  &vardata, &other, &varonleft)) {
        return negate ? (1.0 - DEFAULT_EQ_SEL) : DEFAULT_EQ_SEL;
    }

    // Choose estimation method based on operand type
    if (IsA(other, Const)) {
        // More precise estimation when comparing with constant
        selec = var_eq_const(&vardata, operator, collation,
                             ((Const *) other)->constvalue,
                             ((Const *) other)->constisnull,
                             varonleft, negate);
    } else {
        // Variable-to-variable comparison estimation
        selec = var_eq_non_const(&vardata, operator, collation, other,
                                 varonleft, negate);
    }

    ReleaseVariableStats(vardata);
    return selec;
}
```