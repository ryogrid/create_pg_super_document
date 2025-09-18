# ApplyRetrieveRule

## Location
[src/backend/rewrite/rewriteHandler.c:1701-1880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1701-L1880)

## Overview
Expands an ON SELECT rule (view definition) by converting the view's RTE to a subquery RTE containing the view's underlying query.

## Definition


## Detailed Description
This function implements view expansion by taking an ON SELECT rule and transforming the query to use the view's definition as a subquery. The process involves several sophisticated steps:

1. **View access restrictions**: Checks if non-system view access is restricted and enforces the restriction
2. **Result relation handling**: For views as result relations (UPDATE/DELETE/MERGE), creates a copy of the RTE to serve as the target while expanding the original for source data
3. **RETURNING clause adjustment**: Modifies RETURNING list variables to reference the new result relation for NEW values
4. **Whole-row variable addition**: Adds a resjunk whole-row variable for INSTEAD OF triggers to access OLD values
5. **Lock management**: Handles FOR UPDATE/SHARE clauses by propagating them to the view's underlying tables
6. **Recursive expansion**: Recursively expands any nested view references within the view
7. **Column count adjustment**: Handles CREATE OR REPLACE VIEW scenarios where column counts may have changed

## Parameters / Member Variables
- : The query being rewritten that references the view
- : The ON SELECT rule defining the view
- : The range table index of the view being expanded
- : The view relation being expanded
- : List of active Rules in Rangetable (for recursion detection)

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - rt_fetch
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - makeWholeRowVar
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [get_parse_rowmark](../g/get_parse_rowmark.md)
  - [AcquireRewriteLocks](AcquireRewriteLocks.md)
  - [markQueryForLocking](../m/markQueryForLocking.md)
  - [fireRIRrules](../f/fireRIRrules.md)
  - RelationIsSecurityView
  - [ExecCleanTargetListLength](../E/ExecCleanTargetListLength.md)
  - [makeString](../m/makeString.md)
- Called from:
  - [fireRIRrules](../f/fireRIRrules.md)

## Notes and Other Information
- Only handles single-action ON SELECT rules without qualifications
- Supports INSTEAD OF triggers for views used as result relations in UPDATE/DELETE/MERGE
- Preserves view relation metadata (relid, relkind, etc.) for permission checking and locking
- Handles security barrier views by setting the security_barrier flag
- Creates dummy column names ("?column?") when view definitions are expanded with new columns
- Propagates row security flags from the view query to the parent query
- For INSERT operations on views as result relations, returns the query unchanged to rely on INSTEAD OF triggers