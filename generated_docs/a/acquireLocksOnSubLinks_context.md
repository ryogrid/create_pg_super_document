# acquireLocksOnSubLinks_context

## Location
[src/backend/rewrite/rewriteHandler.c:56-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L56-L59)

## Overview
A context structure used to pass state information during the process of acquiring locks on sublinks within query rewriting operations.

## Definition


## Detailed Description
The  structure serves as a context container that carries state information during the traversal of query trees to acquire necessary locks on sublinks. It specifically maintains the  parameter from the  function, which indicates whether the locks are being acquired for actual query execution or just for planning purposes. This distinction is important because different types of locks may be required depending on the intended use of the query.

## Parameters / Member Variables
- : Boolean flag indicating whether locks are being acquired for query execution (true) or planning (false), passed down from AcquireRewriteLocks function

## Dependencies
- Functions called/Symbols referenced:
  - (None directly referenced)
- Called from (representative examples):
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md)
  - [acquireLocksOnSubLinks](acquireLocksOnSubLinks.md)
  - [rewriteRuleAction](../r/rewriteRuleAction.md)
  - [fireRIRrules](../f/fireRIRrules.md)
  - [CopyAndAddInvertedQual](../C/CopyAndAddInvertedQual.md)
  - [rewriteTargetView](../r/rewriteTargetView.md)

## Notes and Other Information
- Used as a context parameter in tree walking functions during lock acquisition
- Ensures consistent lock acquisition behavior throughout the rewrite process
- Part of PostgreSQL's query rewriting and rule system infrastructure
- Location: src/backend/rewrite/rewriteHandler.c:56-59