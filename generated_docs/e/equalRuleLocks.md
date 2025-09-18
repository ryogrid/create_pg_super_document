# equalRuleLocks

## Location
src/backend/utils/cache/relcache.c: 908 - 952

## Overview
Determines whether two RuleLock structures are equivalent by comparing their rules and associated metadata.

## Definition


## Detailed Description
This function performs a deep comparison of two RuleLock structures to determine if they contain equivalent rule sets. It assumes that rule ordering is repeatable (since PostgreSQL 7.3) because RelationBuildRuleLock reads rules in a consistent order, allowing direct slot-by-slot comparison.

The function handles null pointer cases and compares the number of rules before iterating through each rule to compare their individual properties including rule ID, event type, enabled status, instead flag, qualification conditions, and actions.

## Parameters / Member Variables
- : First RuleLock structure to compare (may be NULL)
- : Second RuleLock structure to compare (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - RuleLock (structure type)
  - RewriteRule (structure type) 
  - equal (function for comparing Node structures)
- Called from (representative examples):
  - RelationClearRelation

## Notes and Other Information
- The comment suggests this function should probably be moved to the rules code module rather than relcache.c
- Since PostgreSQL 7.3, the function relies on consistent rule ordering from RelationBuildRuleLock
- Returns false if either structure is NULL while the other is not
- Performs comprehensive comparison of all rule properties including qual and actions using the equal() function for Node comparison