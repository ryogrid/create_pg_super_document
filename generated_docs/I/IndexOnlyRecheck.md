# IndexOnlyRecheck

## Location
src/backend/executor/nodeIndexonlyscan.c: 325 - 335

## Overview
A stub function that explicitly prevents EvalPlanQual rechecking operations in index-only scans by throwing an error when called.

## Definition


## Detailed Description
IndexOnlyRecheck is a deliberately non-functional access method routine that serves as a safety mechanism in the executor framework. The function exists to maintain interface compatibility with the scan node framework but is designed to never actually execute successfully.

The fundamental issue is that EvalPlanQual (EPQ) operations require CTID (tuple identifier) information to recheck tuples in target relations during concurrent updates. However, index-only scans work exclusively with index data and do not provide CTID information that would be necessary for EPQ processing. If EPQ were to call this function, it would pass heap tuple data rather than the expected index tuple data, creating a data type mismatch.

Rather than attempting to handle this impossible situation, the function immediately throws an error to indicate that the operation is fundamentally incompatible with index-only scan semantics.

## Parameters / Member Variables
- : IndexOnlyScanState containing the scan state (unused, as function immediately errors)
- : TupleTableSlot containing tuple data (unused, as function immediately errors)

## Dependencies
- Functions called/Symbols referenced:
  - elog: PostgreSQL logging/error reporting function used to throw the error
- Called from (representative examples):
  - ExecIndexOnlyScan: Sets this as the recheck function pointer in the scan tuple table slot

## Notes and Other Information
- This function should never actually be called in normal operation due to the architectural incompatibility
- The return statement after elog is present only to satisfy compiler warnings, as elog(ERROR) does not return
- Part of the executor node method table interface for consistency with other scan types
- Demonstrates PostgreSQL's defensive programming approach where impossible operations are explicitly forbidden rather than silently failing