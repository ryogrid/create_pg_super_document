# RuleLock

## Location
[src/include/rewrite/prs2lock.h:40-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/rewrite/prs2lock.h#L40-L44)

## Overview
RuleLock is a structure that contains all rewrite rules that apply to a particular relation in PostgreSQL's rule system, serving as a container for managing multiple RewriteRule instances.

## Definition


## Detailed Description
RuleLock represents a collection of all rewrite rules associated with a specific relation (table or view). Despite its name containing "Lock", it is not actually a locking mechanism but rather a data structure that groups related rewrite rules together. The name is kept for historical reasons from earlier PostgreSQL versions when the rule system had different semantics.

Each relation that has associated rewrite rules will have a corresponding RuleLock structure that contains pointers to all the RewriteRule structures for that relation. This provides an efficient way to access and manage all rules that might apply during query rewriting for a particular table or view.

## Parameters / Member Variables
- : The number of rewrite rules contained in this RuleLock structure
- : An array of pointers to RewriteRule structures, each representing an individual rewrite rule for the relation

## Dependencies
- Functions called/Symbols referenced:
  - [RewriteRule](RewriteRule.md)

- Called from (representative examples):
  - [RelationBuildRuleLock](RelationBuildRuleLock.md)
  - [fireRIRrules](../f/fireRIRrules.md)
  - [matchLocks](../m/matchLocks.md)
  - [relation_is_updatable](../r/relation_is_updatable.md)
  - [equalRuleLocks](../e/equalRuleLocks.md)

## Notes and Other Information
- [RuleLock](RuleLock.md) structures are typically stored as part of RelationData structures (relation cache entries)
- The name "RuleLock" is historical and does not indicate any locking mechanism in current PostgreSQL versions
- Each relation can have multiple rules for different events (SELECT, INSERT, UPDATE, DELETE)
- [RuleLock](RuleLock.md) provides the primary interface for the query rewriter to access rules associated with a relation
- The structure is built and cached as part of relation cache management to avoid repeated catalog lookups
- Views typically have at least one SELECT rule in their RuleLock, which defines how SELECT queries on the view should be rewritten to access underlying tables