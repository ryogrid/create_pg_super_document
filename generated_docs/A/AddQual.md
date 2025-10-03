# AddQual

## Location
[src/backend/rewrite/rewriteManip.c:1057-1124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1057-L1124)

## Overview
Adds a qualifier condition to a query's WHERE clause, with special handling for utility statements and set operations, commonly used in rule processing and query rewriting.

## Definition

```c
void
AddQual(Query *parsetree, Node *qual)
```
## Detailed Description
This function safely adds a qualification condition to the WHERE clause of a query. It handles various edge cases and query types:

**Utility Statements**: For utility commands like NOTIFY, the function silently ignores the qualifier since there's no meaningful WHERE clause. For other utility statements, it raises an error since conditional execution is typically not desired or supported.

**Set Operations**: For UNION/INTERSECT/EXCEPT queries, the function raises an error because there's no appropriate place to add the qualifier condition in the current implementation.

**Regular Queries**: For standard DML queries, the function:
1. Creates a copy of the qualifier condition using copyObject
2. Combines it with any existing WHERE clause using make_and_qual
3. Validates that no aggregates were inadvertently added to the WHERE clause
4. Updates the query's hasSubLinks flag if the added qualifier contains subqueries

The function is primarily used during rule processing where additional conditions need to be dynamically added to queries, such as when applying rule qualifiers or when rewriting target views.

## Parameters / Member Variables
- `*parsetree`: The Query structure to modify
- `*qual`: The qualification condition (Node) to add to the WHERE clause; if NULL, the function returns without making changes
## Dependencies
- Functions called/Symbols referenced:
  - CMD_UTILITY (command type constant)
  - [NotifyStmt](../N/NotifyStmt.md) (utility statement type)
  - copyObject (deep copy function for parse tree nodes)
  - [make_and_qual](../m/make_and_qual.md) (creates AND combination of qualifiers)
  - [contain_aggs_of_level](../c/contain_aggs_of_level.md) (checks for aggregate functions)
  - [checkExprHasSubLink](../c/checkExprHasSubLink.md) (detects sublinks in expressions)
- Called from (representative examples):
  - [rewriteRuleAction](../r/rewriteRuleAction.md) (during rule action processing)
  - [rewriteTargetView](../r/rewriteTargetView.md) (during view rewriting)
  - [AddInvertedQual](AddInvertedQual.md) (for adding inverted qualifiers)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1057-1124
- Returns early without modification if qual parameter is NULL
- Creates a copy of the qualifier to avoid modifying the original
- Includes safety checks to prevent invalid query structures
- Part of PostgreSQL's rule and rewriting system
- Special handling for NOTIFY statements allows rules to execute even when qualifiers would normally prevent them
- Maintains query metadata (hasSubLinks flag) to ensure proper query processing downstream
- Used extensively in rule processing where dynamic addition of WHERE conditions is common

## Simplified Source

```c
void
AddQual(Query *parsetree, Node *qual)
{
    Node       *copy;

    if (qual == NULL)
        return;

    if (parsetree->commandType == CMD_UTILITY)
    {
        // Special case: silently ignore qual for NOTIFY statements
        if (parsetree->utilityStmt && IsA(parsetree->utilityStmt, NotifyStmt))
            return;
        else
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("conditional utility statements are not implemented")));
    }

    if (parsetree->setOperations != NULL)
    {
        // Cannot add quals to set operations (UNION/INTERSECT/EXCEPT)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));
    }

    // Copy the qualifier and add to WHERE clause
    copy = copyObject(qual);
    parsetree->jointree->quals = make_and_qual(parsetree->jointree->quals, copy);

    // Verify no aggregates were added to WHERE clause
    Assert(!contain_aggs_of_level(copy, 0));

    // Update hasSubLinks flag if qualifier contains sublinks
    if (!parsetree->hasSubLinks)
        parsetree->hasSubLinks = checkExprHasSubLink(copy);
}
```