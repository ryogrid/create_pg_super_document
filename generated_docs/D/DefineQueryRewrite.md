# DefineQueryRewrite

## Location
[src/backend/rewrite/rewriteDefine.c:224-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L224-L505)

## Overview
DefineQueryRewrite is the core function that creates rewrite rules in PostgreSQL, performing comprehensive validation and enforcement of rule constraints before inserting the rule into the system catalog.

## Definition

```c
ObjectAddress
DefineQueryRewrite(const char *rulename,
				   Oid event_relid,
				   Node *event_qual,
				   CmdType event_type,
				   bool is_instead,
				   bool replace,
				   List *action)
```
## Detailed Description
DefineQueryRewrite implements the comprehensive logic for creating rewrite rules with extensive validation and constraint checking. It handles different rule types (SELECT vs non-SELECT) with specific restrictions for each, validates permissions and relation types, enforces PostgreSQL's rule system constraints, and manages rule installation. For SELECT rules, it enforces view-specific restrictions including single action requirements, target list matching, and proper naming conventions. For non-SELECT rules, it validates RETURNING list constraints and prevents misuse of reserved rule names. The function also manages locking, dependency tracking, and catalog updates to ensure rule integrity.

## Parameters / Member Variables
- : The name to assign to the rule (must be _RETURN for SELECT rules)
- : OID of the relation the rule applies to
- : Optional qualification condition for rule firing (NULL if no condition)
- : The type of event that triggers the rule (CMD_SELECT, CMD_INSERT, etc.)
- : Boolean indicating if this is an INSTEAD rule
- : Boolean indicating whether to replace an existing rule with the same name
- : List of Query nodes representing the rule's action statements

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - AccessExclusiveLock
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [getInsertSelectQuery](../g/getInsertSelectQuery.md)
  - [checkRuleResultList](../c/checkRuleResultList.md)
  - [InsertRule](../I/InsertRule.md)
  - [SetRelationRuleStatus](../S/SetRelationRuleStatus.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [DefineViewRules](DefineViewRules.md)
  - [DefineRule](DefineRule.md)

## Notes and Other Information
- Uses AccessExclusiveLock on the target relation to prevent concurrent access during rule definition
- Enforces strict constraints for SELECT rules that implement views, including single action requirements and target list validation
- Prevents rule actions that modify OLD or NEW pseudo-relations (not implemented in PostgreSQL)
- Validates that only appropriate relation types (tables, views, materialized views, partitioned tables) can have rules
- Handles backwards compatibility for rule names from older PostgreSQL versions
- For non-SELECT rules, validates RETURNING list constraints and prevents multiple RETURNING lists
- Updates the relation's relhasrules flag in pg_class to trigger cache invalidation across all backends
- Returns an ObjectAddress for the created rule to support dependency tracking and object management