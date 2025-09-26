# get_first_col_type

## Location
[src/backend/optimizer/plan/subselect.c:118-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L118-L161)

## Overview
Extracts the datatype, typmod, and collation information from the first column of a plan's output, primarily used for ARRAY_SUBLINK execution and expression type functions.

## Definition

```c
static void
get_first_col_type(Plan *plan, Oid *coltype, int32 *coltypmod,
				   Oid *colcollation)
```
## Detailed Description
This function retrieves the essential type information (datatype, type modifier, and collation) from the first column of a given plan's target list. The information is stored for later use by ARRAY_SUBLINK execution and by expression type functions like exprType(), exprTypmod(), and exprCollation(), which cannot directly access the plan associated with a SubPlan node. While the information is primarily needed for EXPR_SUBLINK and ARRAY_SUBLINK subplans, it is consistently saved for all subplan types.

The function handles edge cases such as EXISTS queries where the target list might be empty, defaulting to VOID type in such scenarios. It also skips resjunk entries when determining the column type.

## Parameters / Member Variables
- : Input plan whose first column type information is to be extracted
- : Output parameter to store the OID of the column's datatype
- : Output parameter to store the type modifier of the column
- : Output parameter to store the collation OID of the column

## Dependencies
- Functions called/Symbols referenced:
  - linitial_node
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [exprCollation](../e/exprCollation.md)
- Called from (representative examples):
  - [build_subplan](../b/build_subplan.md)
  - [SS_process_ctes](../S/SS_process_ctes.md)
  - [SS_make_initplan_from_plan](../S/SS_make_initplan_from_plan.md)

## Notes and Other Information
- The function is defined as static, meaning it's only accessible within the subselect.c file
- In cases where the target list is empty or contains only resjunk entries, the function defaults to VOIDOID type with invalid typmod and collation
- The function specifically looks for the first non-resjunk entry in the target list
- Located in src/backend/optimizer/plan/subselect.c:118-161