# ExecIndexOnlyRestrPos

## Location
[src/backend/executor/nodeIndexonlyscan.c:470-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L470-L505)

## Overview
ExecIndexOnlyRestrPos restores an index-only scan to a previously marked position, complementing ExecIndexOnlyMarkPos, with special handling for EvalPlanQual (EPQ) recheck scenarios.

## Definition
```c
void ExecIndexOnlyRestrPos(IndexOnlyScanState *node)
```

## Detailed Description
This function restores an index-only scan to a position that was previously marked by ExecIndexOnlyMarkPos. Like its companion marking function, it includes sophisticated logic to handle EvalPlanQual (EPQ) scenarios that occur during concurrent transaction processing.

The function follows the same EPQ handling pattern as ExecIndexOnlyMarkPos. When operating within an EPQ recheck context, it checks whether test tuples exist for the current relation. If test tuples are present (indicated by populated relsubs_slot or relsubs_rowmark arrays), the function skips index access entirely, relying on the EPQ mechanism's assumption that the relsubs_done flag is properly set.

For normal (non-EPQ) operations, the function delegates to the lower-level index_restrpos function to restore the index scan to the previously marked position.

## Parameters / Member Variables
- `node`: Pointer to the IndexOnlyScanState containing the scan state and index scan descriptor to restore

## Dependencies
- Functions called/Symbols referenced:
  - [index_restrpos](../i/index_restrpos.md)
- Types used:
  - [IndexOnlyScanState](../I/IndexOnlyScanState.md)
  - [EState](EState.md)
  - [EPQState](EPQState.md)
  - [Scan](../S/Scan.md)
- Called from (representative examples):
  - [ExecRestrPos](ExecRestrPos.md)

## Notes and Other Information
- This function works in conjunction with ExecIndexOnlyMarkPos to provide cursor-like position management
- EPQ (EvalPlanQual) handling ensures consistency during concurrent transactions
- Contains assertions to validate EPQ state consistency
- The function assumes a position was previously marked using ExecIndexOnlyMarkPos
- Position restoration is essential for implementing features like scrollable cursors in SQL
- Comments reference ExecIndexMarkPos, indicating shared logic and design patterns

## Simplified Source

```c
void ExecIndexOnlyRestrPos(IndexOnlyScanState *node)
{
    EState *estate = node->ss.ps.state;
    EPQState *epqstate = estate->es_epq_active;

    // Handle EvalPlanQual (EPQ) recheck scenarios
    if (estate->es_epq_active != NULL)
    {
        Index scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;

        Assert(scanrelid > 0);
        // Skip index access if EPQ test tuples exist
        if (epqstate->relsubs_slot[scanrelid - 1] != NULL ||
            epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
        {
            // Verify EPQ state consistency
            if (!epqstate->relsubs_done[scanrelid - 1])
                elog(ERROR, "unexpected ExecIndexOnlyRestrPos call in EPQ recheck");
            return;
        }
    }

    // Restore index scan to marked position
    index_restrpos(node->ioss_ScanDesc);
}
```