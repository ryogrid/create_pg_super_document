# ExecIndexRestrPos

## Location
[src/backend/executor/nodeIndexscan.c:850-885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L850-L885)

## Overview
ExecIndexRestrPos restores an index scan to a previously marked position, serving as the counterpart to ExecIndexMarkPos for implementing position-based scan restoration in PostgreSQL's index scanning execution.

## Definition

```c
structure
	 */
	indexstate = makeNode(IndexScanState);
```
## Detailed Description
ExecIndexRestrPos restores an index scan to a position that was previously saved by ExecIndexMarkPos. This function is part of PostgreSQL's execution engine infrastructure that supports position-based scan operations, allowing the executor to backtrack to previously visited positions during query execution.

The function handles EPQ (EvalPlanQual) recheck scenarios specially. During EPQ rechecks, if the scan relation is being substituted with specific tuples (either through relsubs_slot or relsubs_rowmark), the function performs validation checks and returns early without calling the underlying index_restrpos function, since position restoration is not meaningful when using substitute tuples.

For normal execution contexts, the function delegates to the lower-level index_restrpos function to perform the actual position restoration at the index access method level.

## Parameters / Member Variables
- : Pointer to IndexScanState containing the index scan execution state, including the index scan descriptor and EPQ-related information

## Dependencies
- Functions called/Symbols referenced:
  - [index_restrpos](../i/index_restrpos.md)
  - elog (for error reporting)
- Called from (representative examples):
  - [ExecRestrPos](ExecRestrPos.md) (in execAmi.c:380)

## Notes and Other Information
- This function is the counterpart to ExecIndexMarkPos and must be used in conjunction with it
- EPQ (EvalPlanQual) handling ensures that position restoration is not attempted when using substitute tuples during EPQ rechecks
- The function includes assertion checks to validate EPQ state consistency during EPQ rechecks
- Position restoration capability depends on the underlying index access method supporting the mark/restore operations
- Located in src/backend/executor/nodeIndexscan.c:850-885