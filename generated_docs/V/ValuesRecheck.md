# ValuesRecheck

## Location
src/backend/executor/nodeValuesscan.c: 180 - 195

## Overview
ValuesRecheck is a static function that serves as the access method routine to recheck a tuple during EvalPlanQual operations for VALUES scans.

## Definition
```c
static bool ValuesRecheck(ValuesScanState *node, TupleTableSlot *slot)
```

## Detailed Description
ValuesRecheck is a minimalist implementation of the recheck functionality required by PostgreSQL's EvalPlanQual mechanism. Since VALUES clauses generate synthetic data that is not subject to concurrent modifications (unlike table scans), there is nothing that actually needs to be rechecked. The function simply returns true to indicate that the tuple is still valid.

This function is part of the standard scan interface and must be provided even though it performs no actual work for VALUES scans.

## Parameters / Member Variables
- `node`: ValuesScanState containing the scan state (unused in this implementation)
- `slot`: TupleTableSlot containing the tuple to recheck (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - None (function only returns true)
- Called from:
  - ExecValuesScan

## Notes and Other Information
- Always returns true since VALUES data is synthetic and cannot be modified by concurrent transactions
- Required by the scan node interface but performs no actual validation
- Part of PostgreSQL's EvalPlanQual infrastructure for handling concurrent tuple modifications
- The function parameters are present for interface compliance but are not used in the implementation