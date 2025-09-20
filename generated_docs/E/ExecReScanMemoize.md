# ExecReScanMemoize

## Location
[src/backend/executor/nodeMemoize.c:1140-1171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L1140-L1171)

## Overview
ExecReScanMemoize resets the Memoize node for a new scan, managing cache validity when parameters change and coordinating rescanning with the outer plan node.

## Definition

```c
void
ExecReScanMemoize(MemoizeState *node)
```
## Detailed Description
ExecReScanMemoize handles the complexities of restarting a Memoize node scan while preserving cache validity. It resets the execution state machine to begin cache lookups for the new scan and clears scan-specific pointers. The function implements intelligent cache management by distinguishing between parameters that are part of the cache key versus those that are not. When only cache key parameters change, the existing cache remains valid and useful. However, when non-cache-key parameters change, the entire cache must be purged since cached results may no longer be correct for the new parameter values. The function also coordinates with the outer plan node's rescanning, allowing the outer plan to handle its own parameter changes efficiently.

## Parameters / Member Variables
- `node`: The MemoizeState node to rescan, containing the execution state, cache data, and parameter tracking information

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - [ExecReScan](ExecReScan.md)
  - [bms_nonempty_difference](../b/bms_nonempty_difference.md)
  - [cache_purge_all](../c/cache_purge_all.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (main node rescanning dispatcher)

## Notes and Other Information
- Sets execution state to MEMO_CACHE_LOOKUP to begin fresh cache lookups for the new scan
- Nullifies entry and last_tuple pointers to prevent stale references from the previous scan
- Only calls ExecReScan on outer plan if no parameters changed (chgParam == NULL), allowing outer plan to optimize its own rescanning
- Uses bitmap operations (bms_nonempty_difference) to efficiently determine if non-cache-key parameters changed
- Preserves cache entries when only cache key parameters change, maximizing cache reuse across rescans
- Implements a conservative approach by purging the entire cache when any non-cache-key parameter changes, ensuring result correctness