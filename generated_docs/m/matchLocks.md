# matchLocks

## Location
[src/backend/rewrite/rewriteHandler.c:1626-1700](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1626-L1700)

## Overview
Matches a relation's rewrite rules against a specific command type and returns the list of applicable rules, considering replication role and rule enablement status.

## Definition

```c
static List *
matchLocks(CmdType event,
		   Relation relation,
		   int varno,
		   Query *parsetree,
		   bool *hasUpdate)
```
## Detailed Description
This function examines a relation's rewrite rules and returns those that match the specified command type and current execution context. It implements sophisticated filtering logic that considers:

1. **Command type matching**: Rules must match the requested event type
2. **Replication role filtering**: Rules are selectively applied based on the current session's replication role (ORIGIN, LOCAL, or REPLICA)
3. **Rule enablement status**: Disabled rules are excluded from execution
4. **Special handling for SELECT**: ON SELECT rules (views) are always applied regardless of replication role
5. **MERGE command restrictions**: Non-SELECT rules are not supported for MERGE operations
6. **Range table usage**: For SELECT commands, verifies that the relation is actually used in the query

## Parameters / Member Variables
- `event`: The command type (INSERT, UPDATE, DELETE, SELECT, MERGE) to match against
- `relation`: The relation whose rules are being examined
- `varno`: The range table entry number for the relation
- `*parsetree`: The query being processed
- `*hasUpdate`: Output parameter set to true if any UPDATE rules are found
## Dependencies
- Functions called/Symbols referenced:
  - [rangeTableEntry_used](../r/rangeTableEntry_used.md)
  - RelationGetRelationName
  - ereport/errcode/errmsg/errdetail (error reporting)
- Types used:
  - CmdType, RuleLock, RewriteRule
  - SESSION_REPLICATION_ROLE_* constants
  - RULE_* enablement constants
- Called from:
  - [rewriteValuesRTE](../r/rewriteValuesRTE.md)
  - [RewriteQuery](../R/RewriteQuery.md)

## Notes and Other Information
- Returns NIL if the relation has no rules or if no rules match
- ON SELECT rules (for views) bypass replication role restrictions to ensure view functionality
- MERGE commands are incompatible with non-SELECT rules and will raise an error
- The hasUpdate parameter helps callers determine if UPDATE rules are present, which may affect processing decisions
- Rule filtering respects PostgreSQL's replication role system for selective rule application

## Simplified Source

```c
static List *
matchLocks(CmdType event, Relation relation, int varno,
           Query *parsetree, bool *hasUpdate)
{
    RuleLock *rulelocks = relation->rd_rules;
    List *matching_locks = NIL;

    // Return empty list if no rules exist
    if (rulelocks == NULL)
        return NIL;

    // For non-SELECT commands, check result relation matches
    if (parsetree->commandType != CMD_SELECT) {
        if (parsetree->resultRelation != varno)
            return NIL;
    }

    // Iterate through all rules
    for (int i = 0; i < rulelocks->numLocks; i++) {
        RewriteRule *oneLock = rulelocks->rules[i];

        // Track if any UPDATE rules exist
        if (oneLock->event == CMD_UPDATE)
            *hasUpdate = true;

        // Apply replication role filtering for non-SELECT rules
        if (oneLock->event != CMD_SELECT) {
            // Skip disabled rules or wrong replication role
            if (SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA) {
                if (oneLock->enabled == RULE_FIRES_ON_ORIGIN ||
                    oneLock->enabled == RULE_DISABLED)
                    continue;
            } else {
                if (oneLock->enabled == RULE_FIRES_ON_REPLICA ||
                    oneLock->enabled == RULE_DISABLED)
                    continue;
            }

            // MERGE doesn't support non-SELECT rules
            if (parsetree->commandType == CMD_MERGE)
                ereport(ERROR, "MERGE not supported for relations with rules");
        }

        // Add matching rules to result list
        if (oneLock->event == event) {
            if (parsetree->commandType != CMD_SELECT ||
                rangeTableEntry_used((Node *) parsetree, varno, 0))
                matching_locks = lappend(matching_locks, oneLock);
        }
    }

    return matching_locks;
}
```