# setRuleCheckAsUser_Query

## Location
[src/backend/rewrite/rewriteDefine.c:651-690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L651-L690)

## Overview
A static function that sets the checkAsUser field to a specified user ID in all RTEPermissionInfo structures within a Query node and recursively processes all subqueries.

## Definition

```c
static void
setRuleCheckAsUser_Query(Query *qry, Oid userid)
```
## Detailed Description
This function is responsible for updating permission checking behavior within PostgreSQL Query structures. It systematically traverses a Query node to set the checkAsUser field in all RTEPermissionInfo structures, ensuring that permission checks are performed as the specified user rather than the current user. The function handles three main areas: direct RTEPermissionInfos in the query, subqueries in range table entries (RTEs), and Common Table Expressions (CTEs). Additionally, it processes sublinks within the query tree using a specialized walker function.

## Parameters / Member Variables
- : Pointer to the Query structure to be processed
- : The Oid of the user to be set in checkAsUser fields for permission checking

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_node (macro for RTEPermissionInfo)
  - lfirst (list iteration)
  - [setRuleCheckAsUser_Query](setRuleCheckAsUser_Query.md) (recursive calls)
  - castNode (Query casting)
  - query_tree_walker
  - [setRuleCheckAsUser_walker](setRuleCheckAsUser_walker.md)
- Called from (representative examples):
  - [setRuleCheckAsUser_walker](setRuleCheckAsUser_walker.md)
  - [setRuleCheckAsUser_Query](setRuleCheckAsUser_Query.md) (recursive calls)

## Notes and Other Information
- Processes three types of query components: RTEPermissionInfos, subquery RTEs, and CTEs
- Uses recursive calls to handle nested subqueries and WITH clauses
- The QTW_IGNORE_RC_SUBQUERIES flag prevents double-processing of subqueries already handled explicitly
- Part of PostgreSQL's security model for rule-based query rewriting
- Only processes RTE_SUBQUERY type range table entries, ignoring other RTE types
- The function is static, limiting its scope to the rewriteDefine.c file