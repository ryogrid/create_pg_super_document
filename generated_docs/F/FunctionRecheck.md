# FunctionRecheck

## Location
[src/backend/executor/nodeFunctionscan.c:249-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeFunctionscan.c#L249-L264)

## Overview
FunctionRecheck is an access method routine used to recheck a tuple during EvalPlanQual operations in function scans.

## Definition


## Detailed Description
FunctionRecheck is a minimal implementation that always returns true, indicating that no additional tuple validation is needed during EvalPlanQual operations. This is appropriate for function scans because:

1. Function scan results are deterministic - the same function call with the same parameters will always return the same results
2. There are no underlying table rows that could be modified by concurrent transactions
3. The function output is not subject to MVCC visibility rules that would require rechecking

The function serves as a placeholder in the scan method interface, ensuring compatibility with PostgreSQL's executor framework while acknowledging that function scan tuples don't require revalidation.

## Parameters / Member Variables
- : FunctionScanState containing the function scan state (unused in this implementation)
- : TupleTableSlot containing the tuple to recheck (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - None (trivial implementation)
- Called from (representative examples):
  - [ExecFunctionScan](../E/ExecFunctionScan.md)

## Notes and Other Information
- Always returns true, indicating the tuple is still valid
- Required as part of the scan method interface but functionally unnecessary for function scans
- Function scan results are immutable and deterministic, so no rechecking is needed
- Part of the EvalPlanQual mechanism for handling concurrent transaction conflicts