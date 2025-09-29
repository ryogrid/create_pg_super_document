# CopyAndAddInvertedQual

## Location
[src/backend/rewrite/rewriteHandler.c:2311-2380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L2311-L2380)

## Overview
Modifies a query by adding an inverted qualification ('AND rule_qual IS NOT TRUE') to generate suitable else clauses for conditional INSTEAD rules.

## Definition

```c
structuring so that
	 * we only need to process the qual this way once.)
	 */
	(void) acquireLocksOnSubLinks(new_qual, &context);
```
## Detailed Description
CopyAndAddInvertedQual is used in PostgreSQL's rule rewriting system to create the "else" condition for conditional INSTEAD rules. When a rule has a qualification that determines when it should fire, this function creates the inverse condition for cases when the rule should NOT fire. The function:

1. Creates a safe copy of the rule qualification to avoid modifying cached data
2. Processes any subqueries in the qualification by acquiring necessary locks
3. Transforms OLD and NEW references to appropriate variable references
4. Inverts the qualification using 'IS NOT TRUE' semantics (rather than simple NOT)
5. Adds the inverted qualification to the query's WHERE clause

The use of 'IS NOT TRUE' instead of 'NOT' is crucial because it properly handles NULL values - when the original qualification evaluates to NULL, 'IS NOT TRUE' will evaluate to TRUE, ensuring correct three-valued logic behavior.

## Parameters / Member Variables
- : The Query structure to modify with the inverted qualification
- : The original rule qualification to invert (from relcache)
- : Range table index of the relation the rule applies to
- : Command type (INSERT, UPDATE, DELETE) for proper NEW reference handling

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - [acquireLocksOnSubLinks](../a/acquireLocksOnSubLinks.md)
  - [ChangeVarNodes](ChangeVarNodes.md)
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md)
  - [AddInvertedQual](../A/AddInvertedQual.md)
  - rt_fetch
  - PRS2_OLD_VARNO, PRS2_NEW_VARNO (constants)
  - CMD_INSERT, CMD_UPDATE, CMD_DELETE (enums)
  - REPLACEVARS_CHANGE_VARNO, REPLACEVARS_SUBSTITUTE_NULL (constants)
- Called from (representative examples):
  - [fireRules](../f/fireRules.md)

## Notes and Other Information
- Uses 'IS NOT TRUE' instead of 'NOT' to handle NULL values correctly in three-valued logic
- Creates a copy of the qualification to avoid modifying cached rule data
- Handles OLD references by mapping them to the target relation's range table entry
- Handles NEW references differently for INSERT vs UPDATE operations
- Part of the conditional INSTEAD rule processing mechanism
- Acquires locks on subqueries to ensure consistency during rule application
- Critical for generating proper else-clause behavior in conditional rule systems

## Simplified Source

```c
static Query *
CopyAndAddInvertedQual(Query *parsetree,
                       Node *rule_qual,
                       int rt_index,
                       CmdType event)
{
    // Create safe copy to avoid modifying cached rule data
    Node *new_qual = copyObject(rule_qual);

    // Set up context for subquery processing
    acquireLocksOnSubLinks_context context;
    context.for_execute = true;

    // Process any subqueries in the qualification
    (void) acquireLocksOnSubLinks(new_qual, &context);

    // Transform OLD references to point to the target relation
    ChangeVarNodes(new_qual, PRS2_OLD_VARNO, rt_index, 0);

    // Transform NEW references for INSERT/UPDATE operations
    if (event == CMD_INSERT || event == CMD_UPDATE) {
        new_qual = ReplaceVarsFromTargetList(new_qual,
                                             PRS2_NEW_VARNO,
                                             0,
                                             rt_fetch(rt_index, parsetree->rtable),
                                             parsetree->targetList,
                                             (event == CMD_UPDATE) ?
                                             REPLACEVARS_CHANGE_VARNO :
                                             REPLACEVARS_SUBSTITUTE_NULL,
                                             rt_index,
                                             &parsetree->hasSubLinks);
    }

    // Add the inverted qualification to the query
    AddInvertedQual(parsetree, new_qual);

    return parsetree;
}
```