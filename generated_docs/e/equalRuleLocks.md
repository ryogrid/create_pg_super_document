# equalRuleLocks

## Location
[src/backend/utils/cache/relcache.c:908-952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L908-L952)

## Overview
Determines whether two RuleLock structures are equivalent by comparing their rules and associated metadata.

## Definition

```c
static bool
equalRuleLocks(RuleLock *rlock1, RuleLock *rlock2)
```
## Detailed Description
This function performs a deep comparison of two RuleLock structures to determine if they contain equivalent rule sets. It assumes that rule ordering is repeatable (since PostgreSQL 7.3) because RelationBuildRuleLock reads rules in a consistent order, allowing direct slot-by-slot comparison.

The function handles null pointer cases and compares the number of rules before iterating through each rule to compare their individual properties including rule ID, event type, enabled status, instead flag, qualification conditions, and actions.

## Parameters / Member Variables
- `*rlock1`: First RuleLock structure to compare (may be NULL)
- `*rlock2`: Second RuleLock structure to compare (may be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [RuleLock](../R/RuleLock.md) (structure type)
  - [RewriteRule](../R/RewriteRule.md) (structure type) 
  - [equal](equal.md) (function for comparing Node structures)
- Called from (representative examples):
  - [RelationClearRelation](../R/RelationClearRelation.md)

## Notes and Other Information
- The comment suggests this function should probably be moved to the rules code module rather than relcache.c
- Since PostgreSQL 7.3, the function relies on consistent rule ordering from RelationBuildRuleLock
- Returns false if either structure is NULL while the other is not
- Performs comprehensive comparison of all rule properties including qual and actions using the equal() function for Node comparison

## Simplified Source

```c
static bool equalRuleLocks(RuleLock *rlock1, RuleLock *rlock2) {
    // Handle null cases: both null = equal, one null = not equal
    if (rlock1 != NULL) {
        if (rlock2 == NULL)
            return false;

        // Check if rule counts match
        if (rlock1->numLocks != rlock2->numLocks)
            return false;

        // Compare each rule pair in order (ordering is consistent since 7.3)
        for (int i = 0; i < rlock1->numLocks; i++) {
            RewriteRule *rule1 = rlock1->rules[i];
            RewriteRule *rule2 = rlock2->rules[i];

            // Compare all rule properties
            if (rule1->ruleId != rule2->ruleId ||
                rule1->event != rule2->event ||
                rule1->enabled != rule2->enabled ||
                rule1->isInstead != rule2->isInstead ||
                !equal(rule1->qual, rule2->qual) ||
                !equal(rule1->actions, rule2->actions)) {
                return false;
            }
        }
    } else if (rlock2 != NULL) {
        // rlock1 is NULL but rlock2 is not
        return false;
    }

    return true;  // All rules match or both are NULL
}
```