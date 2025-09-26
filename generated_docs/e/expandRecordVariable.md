# expandRecordVariable

## Location
[src/backend/parser/parse_target.c:1519-1703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1519-L1703)

## Overview
Determines the tuple descriptor for a Var of type RECORD by drilling down to find the ultimate defining expression and inferring the tuple structure from it.

## Definition

```c
TupleDesc
expandRecordVariable(ParseState *pstate, Var *var, int levelsup)
```
## Detailed Description
expandRecordVariable handles the complex task of determining the structure of RECORD-type variables, which have no predefined schema. Since PostgreSQL does not allow actual table or view columns to have type RECORD, such variables must refer to JOIN RTEs, FUNCTION RTEs, or subquery outputs.

The function operates through several strategies:

1. **Whole-row references**: When varattno is InvalidAttrNumber, it expands all fields from the referenced RTE using expandRTE and builds a tuple descriptor from the resulting variable list.

2. **RTE drilling**: Based on the RTE type, it recursively resolves the actual expression:
   - **RTE_SUBQUERY**: Examines the corresponding target list entry in the subquery
   - **RTE_JOIN**: Follows the join alias variables to find the underlying expression  
   - **RTE_CTE**: Looks up the corresponding entry in the CTE target list
   - **Other RTE types**: Generally invalid for RECORD variables

3. **Recursive resolution**: For Var expressions found during drilling, it recursively calls itself with appropriate parse state adjustments to handle nested subqueries and CTEs.

4. **Final resolution**: When no further drilling is possible, it delegates to get_expr_result_tupdesc for final type resolution.

## Parameters / Member Variables
- : Parse state containing context information for the current parsing operation
- : The Var node of type RECORD whose tuple descriptor needs to be determined
- : Extra offset for interpreting varlevelsup correctly during recursive calls (outside callers should pass zero)

## Dependencies
- Functions called/Symbols referenced:
  - [GetRTEByRangeTablePosn](../G/GetRTEByRangeTablePosn.md)
  - [expandRTE](expandRTE.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md)
  - [get_tle_by_resno](../g/get_tle_by_resno.md)
  - [GetCTEForRTE](../G/GetCTEForRTE.md)
  - GetCTETargetList
  - [get_expr_result_tupdesc](../g/get_expr_result_tupdesc.md)
  - [exprType](exprType.md)
  - [exprTypmod](exprTypmod.md)
  - [exprCollation](exprCollation.md)
  - [list_nth](../l/list_nth.md)
  - InvalidAttrNumber
  - RTE constants (RTE_RELATION, RTE_SUBQUERY, RTE_JOIN, etc.)
- Called from (representative examples):
  - [ExpandRowReference](../E/ExpandRowReference.md)
  - [ParseComplexProjection](../P/ParseComplexProjection.md)
  - [expandRecordVariable](expandRecordVariable.md) (recursive calls)

## Notes and Other Information
- This function is crucial for PostgreSQL's flexible type system, allowing complex nested queries and joins to work with RECORD types
- The function includes sophisticated parse state management for handling nested subqueries and CTEs, creating temporary parse states as needed
- Self-referencing CTEs receive special handling and are not expanded to prevent infinite recursion
- The function performs extensive validation, generating errors when RECORD variables are found in inappropriate contexts
- Performance consideration: The function may need to traverse complex query structures, but this is necessary for type safety
- The recursive nature allows handling arbitrarily nested structures while maintaining proper variable resolution context