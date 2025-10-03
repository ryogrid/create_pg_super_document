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
- `*rulename`: The name to assign to the rule (must be _RETURN for SELECT rules)
- `event_relid`: OID of the relation the rule applies to
- `*event_qual`: Optional qualification condition for rule firing (NULL if no condition)
- `event_type`: The type of event that triggers the rule (CMD_SELECT, CMD_INSERT, etc.)
- `is_instead`: Boolean indicating if this is an INSTEAD rule
- `replace`: Boolean indicating whether to replace an existing rule with the same name
- `*action`: List of Query nodes representing the rule's action statements
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
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

## Simplified Source

```c
ObjectAddress DefineQueryRewrite(const char *rulename, Oid event_relid, Node *event_qual,
                                CmdType event_type, bool is_instead, bool replace, List *action) {
    Relation event_relation;
    Oid ruleId = InvalidOid;
    ObjectAddress address;

    // Lock the target relation exclusively
    event_relation = table_open(event_relid, AccessExclusiveLock);

    // Validate relation type (table, view, materialized view, partitioned table)
    if (!(event_relation->rd_rel->relkind == RELKIND_RELATION ||
          event_relation->rd_rel->relkind == RELKIND_MATVIEW ||
          event_relation->rd_rel->relkind == RELKIND_VIEW ||
          event_relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("relation cannot have rules")));
    }

    // Check permissions
    if (!object_ownercheck(RelationRelationId, event_relid, GetUserId())) {
        aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(event_relation->rd_rel->relkind),
                      RelationGetRelationName(event_relation));
    }

    // Validate no rule actions modify OLD or NEW (not supported)
    foreach(l, action) {
        query = lfirst_node(Query, l);
        if (query->resultRelation == PRS2_OLD_VARNO ||
            query->resultRelation == PRS2_NEW_VARNO) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("rule actions on OLD/NEW are not implemented")));
        }
    }

    if (event_type == CMD_SELECT) {
        // SELECT rules must be on views and follow strict constraints
        if (!(event_relation->rd_rel->relkind == RELKIND_VIEW ||
              event_relation->rd_rel->relkind == RELKIND_MATVIEW)) {
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("relation cannot have ON SELECT rules")));
        }

        // Must have exactly one INSTEAD SELECT action
        if (action == NIL || list_length(action) > 1 || !is_instead) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("rules on SELECT must have action INSTEAD SELECT")));
        }

        // Validate target list matches relation structure
        query = linitial_node(Query, action);
        checkRuleResultList(query->targetList, RelationGetDescr(event_relation), true,
                           event_relation->rd_rel->relkind != RELKIND_MATVIEW);

        // Rule must be named "_RETURN" for SELECT rules
        if (strcmp(rulename, ViewSelectRuleName) != 0) {
            rulename = pstrdup(ViewSelectRuleName);
        }
    } else {
        // Non-SELECT rules: validate RETURNING lists
        bool haveReturning = false;
        foreach(l, action) {
            query = lfirst_node(Query, l);
            if (query->returningList) {
                if (haveReturning) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                   errmsg("cannot have multiple RETURNING lists")));
                }
                haveReturning = true;
                checkRuleResultList(query->returningList, RelationGetDescr(event_relation),
                                   false, false);
            }
        }
    }

    // Install the rule if it's valid
    if (action != NIL || is_instead) {
        ruleId = InsertRule(rulename, event_type, event_relid, is_instead,
                           event_qual, action, replace);
        SetRelationRuleStatus(event_relid, true);
    }

    ObjectAddressSet(address, RewriteRelationId, ruleId);
    table_close(event_relation, NoLock);

    return address;
}
```