# repairViewRuleMultiLoop

## Location
src/bin/pg_dump/pg_dump_sort.c: 978 - 1011

## Overview
Repairs circular dependencies in pg_dump by breaking view-rule dependency loops when other objects are involved in the cycle, ensuring proper dump ordering by making the ON SELECT rule a separately-dumped object.

## Definition


## Detailed Description
This function is part of pg_dump's dependency loop repair mechanism. When a circular dependency is detected that involves a view, its ON SELECT rule, and other objects, this function breaks the cycle by:

1. Removing the view's dependency on the rule
2. Marking the view to be printed with a dummy definition (allowing forward reference)
3. Making the rule a separately-dumped object
4. Re-establishing the rule's dependency on the view (ensuring proper order)
5. Moving the rule to the post-data phase since it's now separate

The function specifically handles multi-object loops (as opposed to simple view-rule loops handled by repairViewRuleLoop). It works by temporarily creating a dummy view definition that can be dumped early, while deferring the actual rule to the post-data phase.

## Parameters / Member Variables
- : Pointer to the DumpableObject representing the view involved in the dependency loop
- : Pointer to the DumpableObject representing the ON SELECT rule that needs to be separated from the view

## Dependencies
- Functions called/Symbols referenced:
  - removeObjectDependency (removes existing dependency relationships)
  - addObjectDependency (establishes new dependency relationships)
  - postDataBoundId (global variable marking post-data phase boundary)
  - DumpableObject (base structure for dumpable database objects)
  - TableInfo (structure containing view-specific information)
  - RuleInfo (structure containing rule-specific information)
- Called from:
  - repairDependencyLoop (main dependency loop repair dispatcher)

## Notes and Other Information
- This approach does NOT work for materialized views (matviews) as noted in the comments
- The function assumes that repairViewRuleLoop() may have previously been called and removed the rule's dependency on the view, so it explicitly restores this dependency
- The dummy_view flag causes pg_dump to emit a CREATE VIEW statement without the actual query definition initially
- The separate flag ensures the rule gets its own independent dump operation
- Moving the rule to post-data phase (via postDataBoundId dependency) ensures it's dumped after all table data but before final cleanup operations