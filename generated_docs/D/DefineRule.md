# DefineRule

## Location
[src/backend/rewrite/rewriteDefine.c:190-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L190-L223)

## Overview
DefineRule is the main entry point for executing CREATE RULE commands, serving as a high-level interface that coordinates rule parsing, relation locking, and rule definition.

## Definition

```c
ObjectAddress
DefineRule(RuleStmt *stmt, const char *queryString)
```
## Detailed Description
DefineRule acts as the primary interface for CREATE RULE command execution in PostgreSQL. It performs the initial parsing and transformation of the rule statement through transformRuleStmt, acquires the necessary locks on the target relation, and then delegates the actual rule creation to DefineQueryRewrite. This function bridges the gap between the parser's output (RuleStmt) and the lower-level rule definition machinery, ensuring proper statement transformation and relation access before rule creation.

## Parameters / Member Variables
- `*stmt`: The parsed CREATE RULE statement containing rule name, target relation, event type, conditions, and actions
- `*queryString`: The original SQL command string for error reporting and logging purposes
## Dependencies
- Functions called/Symbols referenced:
  - [transformRuleStmt](../t/transformRuleStmt.md)
  - RangeVarGetRelid
  - AccessExclusiveLock
  - [DefineQueryRewrite](DefineQueryRewrite.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- This function represents the public interface for rule creation and is called by the utility command processor
- Uses AccessExclusiveLock to match the locking level used by DefineQueryRewrite for consistency
- The function performs minimal processing itself, acting primarily as a coordinator between parsing and rule definition phases
- Returns an ObjectAddress identifying the newly created rule object
- Part of the DDL (Data Definition Language) command processing infrastructure

## Simplified Source

```c
ObjectAddress DefineRule(RuleStmt *stmt, const char *queryString) {
    List *actions;
    Node *whereClause;
    Oid relId;

    // Parse and transform the rule statement
    transformRuleStmt(stmt, queryString, &actions, &whereClause);

    // Find and lock the target relation with exclusive access
    relId = RangeVarGetRelid(stmt->relation, AccessExclusiveLock, false);

    // Create the rule using the low-level interface
    return DefineQueryRewrite(stmt->rulename, relId, whereClause,
                             stmt->event, stmt->instead, stmt->replace, actions);
}
```