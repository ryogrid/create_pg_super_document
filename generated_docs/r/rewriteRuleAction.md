# rewriteRuleAction

## Location
src/backend/rewrite/rewriteHandler.c: 349 - 701

## Overview
Rewrites a rule action query by merging it with the triggering query, applying appropriate qualifiers, and adjusting variable references and range table entries.

## Definition
```c
static Query *rewriteRuleAction(Query *parsetree, Query *rule_action, Node *rule_qual, int rt_index, CmdType event, bool *returning_flag)
```

## Detailed Description
rewriteRuleAction is a core function in PostgreSQL's rule rewriting system that transforms a rule action query to incorporate context from the triggering query. This complex process involves multiple phases:

1. **Lock Acquisition**: Acquires necessary locks on all relations referenced in the rule action and qualifiers
2. **Variable Node Offsetting**: Adjusts variable reference numbers (varnos) to accommodate the merged range tables
3. **Range Table Merging**: Combines the original query's range table with the rule action's range table 
4. **LATERAL Marking**: Marks subquery RTEs as LATERAL if they contain references to the current query level
5. **Join Tree Adjustment**: Merges join trees from both queries while handling duplicate references
6. **CTE Handling**: Merges Common Table Expressions while checking for name conflicts
7. **Qualifier Addition**: Adds rule qualifiers and original query qualifiers to the rule action
8. **Target List Rewriting**: Replaces NEW.attribute references with actual target list entries
9. **RETURNING Clause Processing**: Handles RETURNING clauses from both the rule and triggering query

The function ensures proper isolation and variable scoping while maintaining semantic correctness of the rewritten query.

## Parameters / Member Variables
- `parsetree`: The original triggering query
- `rule_action`: The rule action query to be rewritten
- `rule_qual`: WHERE condition of the rule (NULL if unconditional)
- `rt_index`: Range table index of the result relation in the original query
- `event`: Type of rule event (INSERT, UPDATE, DELETE)
- `returning_flag`: Output flag set to true if RETURNING clause is rewritten (must be initialized to false)

## Dependencies
- Functions called/Symbols referenced:
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md)
  - [acquireLocksOnSubLinks](../a/acquireLocksOnSubLinks.md)
  - copyObject
  - [getInsertSelectQuery](../g/getInsertSelectQuery.md)
  - [OffsetVarNodes](../O/OffsetVarNodes.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [adjustJoinTreeList](../a/adjustJoinTreeList.md)
  - [CombineRangeTables](../C/CombineRangeTables.md)
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - [rangeTableEntry_used](rangeTableEntry_used.md)
  - [checkExprHasSubLink](../c/checkExprHasSubLink.md)
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md)
  - [AddQual](../A/AddQual.md)
  - rt_fetch
- Called from (representative examples):
  - [fireRules](../f/fireRules.md)

## Notes and Other Information
- This is a static function, only accessible within rewriteHandler.c
- Creates modifiable copies of input queries to avoid modifying cached versions
- Handles complex edge cases like INSERT...SELECT rules and set operations
- Enforces restrictions such as disallowing conditional UNION/INTERSECT/EXCEPT statements
- Prevents name conflicts between CTEs in original and rule action queries
- Manages proper variable scoping between OLD/NEW references and actual table references
- Critical for PostgreSQL's rule system which enables views, triggers, and other rewrite-based features
- The function maintains hasSubLinks, hasRowSecurity, hasRecursive, and hasModifyingCTE flags correctly across the merge