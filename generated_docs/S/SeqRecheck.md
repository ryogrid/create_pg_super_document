# SeqRecheck

## Location
src/backend/executor/nodeSeqscan.c: 89 - 107

## Overview
SeqRecheck is a static function that serves as an access method routine to recheck a tuple during EvalPlanQual processing for sequential scans.

## Definition
```c
static bool SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
```

## Detailed Description
SeqRecheck is part of PostgreSQL's EvalPlanQual (EPQ) mechanism, which is used to handle concurrent updates during query execution. For sequential scans, this function always returns true because sequential scans do not use search keys during heap_beginscan operations. Unlike IndexScan operations that need to verify that keys are still valid after concurrent modifications, sequential scans do not have key-based constraints to recheck. The function serves as a placeholder in the access method interface but performs no actual validation.

## Parameters / Member Variables
- `node`: SeqScanState pointer containing the scan state information (unused in implementation)
- `slot`: TupleTableSlot pointer containing the tuple to recheck (unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - SeqScanState
- Called from (representative examples):
  - ExecSeqScan

## Notes and Other Information
- This is a static function, only accessible within nodeSeqscan.c
- Always returns true as sequential scans have no keys to validate
- Part of the EvalPlanQual mechanism for handling concurrent tuple modifications
- The comment notes that SeqScan never uses keys in heap_beginscan, which is considered 'very bad' from a design perspective
- Contrasts with IndexScan which performs actual key validation during recheck operations