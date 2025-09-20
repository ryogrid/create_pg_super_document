# ExecForeignScan

## Location
[src/backend/executor/nodeForeignscan.c:118-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L118-L141)

## Overview
ExecForeignScan is the main execution function for foreign scan nodes that fetches tuples from Foreign Data Wrappers, checks local qualifications, and returns the next qualifying tuple.

## Definition

```c
structure
	 */
	scanstate = makeNode(ForeignScanState);
```
## Detailed Description
ExecForeignScan serves as the primary execution interface for foreign scan operations in PostgreSQL's executor framework. It acts as a high-level coordinator that delegates the actual work to the generic ExecScan framework, providing foreign-scan-specific access and recheck methods.

The function follows PostgreSQL's standard scan execution pattern by utilizing the ExecScan infrastructure, which handles common scan operations such as:
- Projection and qualification evaluation
- EvalPlanQual processing coordination
- Memory management and cleanup
- Result tuple formatting

A key aspect of this function is its handling of EvalPlanQual scenarios. When EvalPlanQual is active (indicating concurrent transaction processing), the function specifically ignores direct modification operations (INSERT, UPDATE, DELETE) since these operations are not relevant for EvalPlanQual rechecking and cannot be re-evaluated safely.

The function delegates tuple fetching to ForeignNext and tuple rechecking during EvalPlanQual to ForeignRecheck, maintaining clean separation of concerns within the foreign scan execution pipeline.

## Parameters / Member Variables
- : PlanState structure that is cast to ForeignScanState, containing all execution state information for the foreign scan node including FDW routines, relation information, and execution context

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - [ExecScan](ExecScan.md)
  - [ForeignNext](../F/ForeignNext.md) (via function pointer)
  - [ForeignRecheck](../F/ForeignRecheck.md) (via function pointer)
- Called from:
  - [ExecInitForeignScan](ExecInitForeignScan.md) (for node setup)

## Notes and Other Information
- This is a static function, only accessible within nodeForeignscan.c
- The function leverages PostgreSQL's generic ExecScan framework rather than implementing scan logic directly
- Direct modification operations are filtered out during EvalPlanQual processing for safety and correctness
- The casting from PlanState to ForeignScanState is safe due to PostgreSQL's node type system
- Function pointers to ForeignNext and ForeignRecheck are passed to ExecScan to customize scan behavior for foreign data sources
- The function inherits all the standard scan capabilities (qualification checking, projection, etc.) from ExecScan