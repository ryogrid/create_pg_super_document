# repairViewRuleLoop

## Location
src/bin/pg_dump/pg_dump_sort.c: 958 - 977

## Overview
repairViewRuleLoop resolves circular dependency loops between views (including materialized views) and their ON SELECT rules by removing the implicit rule-to-view dependency.

## Definition


## Detailed Description
repairViewRuleLoop addresses a specific type of circular dependency that occurs between views/materialized views and their ON SELECT rules. This circular dependency arises because:
1. pg_dump forces views to depend on their ON SELECT rules (explicit dependency)
2. There's an implicit dependency in the other direction (rule depends on view)

The function resolves this cycle by removing the implicit dependency from the rule back to the view, while preserving the explicit dependency from the view to the rule. This approach ensures that the ON SELECT rule will be dumped before the view, which is the correct order for restoration.

The function assumes that when this repair is applied, there are no other objects involved in the loop - it's specifically designed for simple two-object cycles between a view and its rule. The dump flags on both objects are already correctly set when this function is called, so no additional flag manipulation is needed.

## Parameters / Member Variables
- : The DumpableObject representing the view or materialized view that's part of the circular dependency
- : The DumpableObject representing the ON SELECT rule that depends on the view

## Dependencies
- Functions called/Symbols referenced:
  - removeObjectDependency
- Called from (representative examples):
  - repairDependencyLoop

## Notes and Other Information
- Specifically handles view-rule circular dependencies in pg_dump
- Applies to both regular views and materialized views 
- Assumes a simple two-object loop with no other dependencies involved
- Preserves the explicit view-to-rule dependency while breaking the implicit rule-to-view dependency
- Relies on pre-set dump flags for correct object handling
- Part of pg_dump's targeted dependency loop resolution system
- Ensures ON SELECT rules are dumped before their associated views for proper restoration order