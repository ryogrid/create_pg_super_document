# matchLocks

## Location
src/backend/rewrite/rewriteHandler.c: 1626 - 1700

## Overview
Matches a relation's rewrite rules against a specific command type and returns the list of applicable rules, considering replication role and rule enablement status.

## Definition


## Detailed Description
This function examines a relation's rewrite rules and returns those that match the specified command type and current execution context. It implements sophisticated filtering logic that considers:

1. **Command type matching**: Rules must match the requested event type
2. **Replication role filtering**: Rules are selectively applied based on the current session's replication role (ORIGIN, LOCAL, or REPLICA)
3. **Rule enablement status**: Disabled rules are excluded from execution
4. **Special handling for SELECT**: ON SELECT rules (views) are always applied regardless of replication role
5. **MERGE command restrictions**: Non-SELECT rules are not supported for MERGE operations
6. **Range table usage**: For SELECT commands, verifies that the relation is actually used in the query

## Parameters / Member Variables
- : The command type (INSERT, UPDATE, DELETE, SELECT, MERGE) to match against
- : The relation whose rules are being examined
- : The range table entry number for the relation
- : The query being processed
- : Output parameter set to true if any UPDATE rules are found

## Dependencies
- Functions called/Symbols referenced:
  - rangeTableEntry_used
  - RelationGetRelationName
  - ereport/errcode/errmsg/errdetail (error reporting)
- Types used:
  - CmdType, RuleLock, RewriteRule
  - SESSION_REPLICATION_ROLE_* constants
  - RULE_* enablement constants
- Called from:
  - rewriteValuesRTE
  - RewriteQuery

## Notes and Other Information
- Returns NIL if the relation has no rules or if no rules match
- ON SELECT rules (for views) bypass replication role restrictions to ensure view functionality
- MERGE commands are incompatible with non-SELECT rules and will raise an error
- The hasUpdate parameter helps callers determine if UPDATE rules are present, which may affect processing decisions
- Rule filtering respects PostgreSQL's replication role system for selective rule application