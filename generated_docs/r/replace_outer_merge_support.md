# replace_outer_merge_support

## Location
[src/backend/optimizer/util/paramassign.c:317-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L317-L366)

## Overview
Generates a Param node to replace a MergeSupportFunc expression that references an outer-level MERGE query, enabling parameter passing for MERGE-specific functions across query levels.

## Definition
```c
Param *replace_outer_merge_support(PlannerInfo *root, MergeSupportFunc *msf)
```

## Detailed Description
This function handles MergeSupportFunc expressions that appear in subqueries but need to reference values from an upper-level MERGE command. MergeSupportFunc expressions are special functions that provide access to MERGE-specific metadata (like which action was performed) and are typically used in RETURNING clauses of MERGE statements.

When a subquery contains a MergeSupportFunc that references an outer MERGE operation, this function:
1. Searches up the query hierarchy to find the appropriate MERGE command level
2. Creates a copy of the MergeSupportFunc for parameter processing
3. Registers the function as a parameter in the MERGE query level's plan parameters
4. Returns a Param node that serves as a placeholder in the current query level

The function includes error checking to ensure that the MergeSupportFunc is indeed associated with a MERGE command somewhere in the query hierarchy, as guaranteed by the parser.

## Parameters / Member Variables
- `root`: PlannerInfo pointer representing the current query level's planning context (must not be a MERGE command itself)
- `msf`: MergeSupportFunc pointer to the merge support function expression that references an outer MERGE query

## Dependencies
- Functions called/Symbols referenced:
  - [exprType](../e/exprType.md): Determines the data type of the MergeSupportFunc expression
  - copyObject: Creates a deep copy of the MergeSupportFunc
  - makeNode: Creates new PlannerParamItem and Param nodes
  - [lappend_oid](../l/lappend_oid.md): Appends parameter type to the global parameter types list
  - [lappend](../l/lappend.md): Adds the parameter item to the plan parameters list
  - elog: Reports error if no MERGE command is found in the query hierarchy

- Called from (representative examples):
  - [replace_correlation_vars_mutator](replace_correlation_vars_mutator.md): Used during correlation variable replacement in subquery planning

## Notes and Other Information
- The function asserts that the current query level is not a MERGE command (root->parse->commandType != CMD_MERGE)
- Searches upward through parent_root chain until finding a MERGE command or reaching NULL (which triggers an error)
- Like replace_outer_grouping, this function creates a new parameter slot for each reference rather than attempting de-duplication
- The resulting Param node uses PARAM_EXEC parameter kind for execution-time evaluation
- Parameter type modifier is set to -1 and collation ID is InvalidOid
- Location information from the original MergeSupportFunc is preserved in the Param node
- [MergeSupportFunc](../M/MergeSupportFunc.md) expressions are typically used to access MERGE action metadata in RETURNING clauses