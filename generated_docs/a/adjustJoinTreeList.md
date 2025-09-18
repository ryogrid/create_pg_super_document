# adjustJoinTreeList

## Location
src/backend/rewrite/rewriteHandler.c: 702 - 763

## Overview
Creates a copy of the query's join tree list and optionally removes a specified range table entry from the top-level join items.

## Definition
```c
static List *adjustJoinTreeList(Query *parsetree, bool removert, int rt_index)
```

## Detailed Description
adjustJoinTreeList is a utility function used during rule rewriting to create a modified copy of a query's FROM clause (jointree). The function serves two primary purposes:

1. **Deep Copy Creation**: Creates a completely separate copy of the join tree that shares no nodes with the original, ensuring that modifications to the copy don't affect the original query structure.

2. **Selective Range Table Removal**: When the removert parameter is true, the function searches for and removes any top-level occurrence of the specified range table index. This is particularly useful in rule rewriting where the original target relation (such as an UPDATE or DELETE target) needs to be excluded from the new query's FROM clause to avoid duplicate references.

The function only examines top-level join items in the FROM list, not nested join structures, which is appropriate since target relations for UPDATE and DELETE operations are expected to appear at the top level of the join tree.

## Parameters / Member Variables
- `parsetree`: The source Query containing the jointree to be copied
- `removert`: Boolean flag indicating whether to attempt removal of the specified range table entry
- `rt_index`: The range table index to remove (only used when removert is true)

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - foreach_delete_current
  - IsA (macro for type checking)
  - RangeTblRef (node type)
- Called from (representative examples):
  - rewriteRuleAction

## Notes and Other Information
- This is a static function, only accessible within rewriteHandler.c
- Returns a completely separate copy sharing no substructure with the original
- Only searches for the target rt_index at the top level of the FROM clause, not within nested joins
- Uses foreach_delete_current for safe list modification during iteration
- Part of PostgreSQL's rule rewriting infrastructure that ensures proper query structure during rule application
- The function breaks after finding and removing the first matching RangeTblRef to avoid unnecessary continued iteration