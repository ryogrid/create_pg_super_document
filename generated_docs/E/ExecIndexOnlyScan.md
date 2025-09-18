# ExecIndexOnlyScan

## Location
src/backend/executor/nodeIndexonlyscan.c: 336 - 362

## Overview
The main execution entry point for index-only scan nodes, coordinating runtime key setup and delegating tuple retrieval to the executor scan framework.

## Definition


## Detailed Description
ExecIndexOnlyScan serves as the primary execution function for index-only scan plan nodes within PostgreSQL's executor framework. The function acts as a thin wrapper that handles runtime key initialization when necessary, then delegates the actual scanning work to the generic ExecScan framework.

The function first checks if runtime keys need to be computed and sets them up by calling ExecReScan if they haven't been prepared yet. Runtime keys are scan keys whose values are determined at execution time rather than planning time, typically involving parameter references or volatile expressions.

After ensuring that runtime keys are ready, the function calls ExecScan with specialized access method functions: IndexOnlyNext for tuple retrieval and IndexOnlyRecheck for EvalPlanQual operations (though the latter will always error for index-only scans).

## Parameters / Member Variables
- : PlanState pointer that will be cast to IndexOnlyScanState, containing all scan state and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - castNode: PostgreSQL macro for safe type casting with debug assertions
  - [ExecReScan](ExecReScan.md): Re-initializes the scan with current runtime key values
  - [ExecScan](ExecScan.md): Generic executor scan framework that coordinates tuple retrieval
  - [IndexOnlyNext](../I/IndexOnlyNext.md): Access method function for retrieving the next tuple
  - [IndexOnlyRecheck](../I/IndexOnlyRecheck.md): Access method function for EvalPlanQual rechecking (always errors)
- Called from (representative examples):
  - [ExecInitIndexOnlyScan](ExecInitIndexOnlyScan.md): Sets this as the execution function during node initialization

## Notes and Other Information
- This function implements the standard PostgreSQL executor node interface
- Runtime key setup is deferred until first execution for efficiency
- The function integrates with PostgreSQL's generic scan execution framework rather than implementing custom logic
- Part of the executor node method table that gets called by the executor engine
- The castNode operation provides type safety and debugging assistance in development builds