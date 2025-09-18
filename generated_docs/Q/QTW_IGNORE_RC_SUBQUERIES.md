# QTW_IGNORE_RC_SUBQUERIES

## Location
src/include/nodes/nodeFuncs.h: 24 - 24

## Overview
A composite flag bit that combines QTW_IGNORE_RT_SUBQUERIES and QTW_IGNORE_CTE_SUBQUERIES, used to ignore both range table and CTE subqueries during query tree traversal.

## Definition
```c
#define QTW_IGNORE_RC_SUBQUERIES    0x03    /* both of above */
```

## Detailed Description
QTW_IGNORE_RC_SUBQUERIES is a convenience flag that combines the functionality of both QTW_IGNORE_RT_SUBQUERIES (0x01) and QTW_IGNORE_CTE_SUBQUERIES (0x02). The value 0x03 is the bitwise OR of these two flags, providing a shorthand way to ignore both range table subqueries and Common Table Expression subqueries during query tree traversal operations.

This flag is commonly used when tree traversal operations need to focus on the main query structure while skipping all types of subqueries that are contained within the query's supporting structures (both range table entries and CTE definitions). This is particularly useful for operations like lock acquisition, rule processing, and security checks that need to analyze query structure without descending into nested subqueries.

## Parameters / Member Variables
- Value: `0x03` - Binary combination of QTW_IGNORE_RT_SUBQUERIES (0x01) and QTW_IGNORE_CTE_SUBQUERIES (0x02)

## Dependencies
- Used by:
  - [setRuleCheckAsUser_Query](../s/setRuleCheckAsUser_Query.md) (src/backend/rewrite/rewriteDefine.c:683)
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md) (src/backend/rewrite/rewriteHandler.c:301)
  - [fireRIRrules](../f/fireRIRrules.md) (src/backend/rewrite/rewriteHandler.c:2171)
  - [rewriteTargetView](../r/rewriteTargetView.md) (src/backend/rewrite/rewriteHandler.c:3442)
  - [checkExprHasSubLink](../c/checkExprHasSubLink.md) (src/backend/rewrite/rewriteManip.c:305)
  - [ScanQueryForLocks](../S/ScanQueryForLocks.md) (src/backend/utils/cache/plancache.c:1912)
- Part of the QTW flag system defined in src/include/nodes/nodeFuncs.h

## Notes and Other Information
- This is a composite flag that effectively sets both QTW_IGNORE_RT_SUBQUERIES and QTW_IGNORE_CTE_SUBQUERIES
- Commonly used in rewrite system operations where subqueries should be ignored
- Used in lock acquisition to avoid analyzing locks needed by subqueries
- Applied in rule processing to focus on main query transformation
- The 'RC' in the name likely stands for 'Range table and CTE' subqueries
- Can still be combined with other QTW flags for more complex traversal control