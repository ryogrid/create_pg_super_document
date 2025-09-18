# expandRTE

## Location
src/backend/parser/parse_relation.c: 2659 - 3016

## Overview
Expands the columns of a Range Table Entry (RTE) by creating lists of column names and corresponding Var nodes for each accessible column, handling different RTE types with specific expansion logic.

## Definition


## Detailed Description
This function is a central component of PostgreSQL's query processing system that expands RTE columns into usable column name lists and Var node lists. It handles multiple types of RTEs with type-specific expansion logic:

1. **RTE_RELATION**: Regular table relations - delegates to expandRelation()
2. **RTE_SUBQUERY**: Subquery relations - processes the subquery's target list
3. **RTE_FUNCTION**: Function calls in FROM clause - handles various function return types (scalar, composite, record)
4. **RTE_JOIN**: Join relations - processes join alias variables and column names
5. **RTE_TABLEFUNC/RTE_VALUES/RTE_CTE/RTE_NAMEDTUPLESTORE**: Special table constructs - uses stored column type information
6. **RTE_RESULT**: Result relations - exposes no columns

The function provides flexibility in output by allowing callers to request only column names, only Var nodes, or both. It also handles dropped columns based on the include_dropped parameter, either omitting them or including them as empty strings/NULL constants.

## Parameters / Member Variables
- : The Range Table Entry to expand, containing relation information and metadata
- : The range table index (varno) to use in created Var nodes, typically matching the RTE's position
- : The varlevelsup value for created Var nodes, indicating nesting level in subqueries
- : Source location information to attach to created Var nodes for error reporting
- : Boolean flag determining whether to include dropped columns (as empty strings/NULL constants) or omit them
- : Output parameter for list of column name strings (pass NULL if not needed)
- : Output parameter for list of Var nodes representing columns (pass NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - expandRelation (for RTE_RELATION)
  - expandTupleDesc (for composite function types)
  - makeVar (creating Var nodes)
  - makeString (creating string nodes)
  - makeNullConst (creating null constants for dropped columns)
  - get_expr_result_type (determining function return types)
  - exprType, exprTypmod, exprCollation (extracting expression type information)
- Data structures used:
  - RangeTblEntry, RangeTblFunction, TargetEntry
  - Various PostgreSQL list manipulation functions
- Called from (representative examples):
  - transformWholeRowRef (expanding whole-row references)
  - expandRecordVariable (expanding record variables)
  - build_physical_tlist (optimizer planning)
  - set_relation_column_names (rule utilities)

## Notes and Other Information
- Only user columns are considered; system columns are excluded from expansion
- The function handles complex scenarios like function calls returning composite types, record types, or scalar values
- For JOIN RTEs, the function processes joinaliasvars to handle JOIN USING columns correctly
- Includes extensive error checking, such as verifying subquery target list consistency
- The ordinality column for functions with ORDINALITY is handled as a special case (INT8OID type)
- Critical for query rewriting, planning, and execution phases where column information needs to be materialized
- Part of PostgreSQL's namespace resolution system that translates abstract relation references into concrete column lists