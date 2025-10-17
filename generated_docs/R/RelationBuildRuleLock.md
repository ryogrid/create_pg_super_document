# RelationBuildRuleLock

## Location
[src/backend/utils/cache/relcache.c:733-907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L733-L907)

## Overview
RelationBuildRuleLock constructs the rewrite rule structures for a relation by scanning pg_rewrite and parsing rule definitions into executable form.

## Definition
static void RelationBuildRuleLock(Relation relation)

## Detailed Description
RelationBuildRuleLock builds the complete rule lock structure (rd_rules) for a relation by scanning the pg_rewrite system catalog and constructing parsed rule trees. The function creates a dedicated memory context for rule data to enable efficient cleanup when the relcache entry is flushed. It processes each rule by extracting the rule definition strings, parsing them into node trees, and setting up appropriate security context information based on view security settings.

The function handles the complex task of converting textual rule representations back into executable parse trees, managing memory carefully to avoid leaks from large rule strings that may be TOASTed. It also implements security invoker semantics for views by setting appropriate checkAsUser fields in the rule trees.

## Parameters / Member Variables
- : The relation descriptor whose rules (rd_rules) will be populated from pg_rewrite catalog data

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md), RuleLock, RewriteRule (data structure types)
  - AllocSetContextCreate, ALLOCSET_SMALL_SIZES (memory context creation)
  - MemoryContextCopyAndSetIdentifier, MemoryContextAlloc (memory management)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext (system catalog scanning)
  - Form_pg_rewrite (pg_rewrite tuple structure type)
  - [heap_getattr](../h/heap_getattr.md) (attribute extraction from tuples)
  - TextDatumGetCString, stringToNode (text parsing and node tree construction)
  - CMD_SELECT, RELKIND_VIEW (constants for rule type checking)
  - RelationHasSecurityInvoker (security invoker view detection)
  - [setRuleCheckAsUser](../s/setRuleCheckAsUser.md) (security context setup for rule trees)
  - [repalloc](../r/repalloc.md), MemoryContextDelete (memory management utilities)
- Called from (representative examples):
  - [RelationBuildDesc](RelationBuildDesc.md) (during complete relation descriptor construction)

## Notes and Other Information
- Creates a dedicated memory context (rd_rulescxt) for rule data to enable efficient cleanup
- Scans rules in name order using RewriteRelRulenameIndexId for consistent rule firing order
- Handles TOAST decompression for large rule strings and manages memory carefully to prevent leaks
- Implements security invoker semantics for SELECT rules on security_invoker views
- Sets checkAsUser fields in rule trees during loading rather than storage for ALTER TABLE OWNER efficiency
- Dynamically expands rule array as needed with repalloc when maxlocks is exceeded
- Handles the case where relhasrules may be out-of-date by checking for zero rules
- Parses both rule actions and qualifications from textual representations into node trees
- Essential for view and rule functionality in PostgreSQL's query rewrite system

## Simplified Source

```c
static void RelationBuildRuleLock(Relation relation) {
    // Create dedicated memory context for rule data
    MemoryContext rulescxt = AllocSetContextCreate(CacheMemoryContext,
                                                  "relation rules",
                                                  ALLOCSET_SMALL_SIZES);
    relation->rd_rulescxt = rulescxt;
    MemoryContextCopyAndSetIdentifier(rulescxt, RelationGetRelationName(relation));

    // Initialize rule array (dynamically expandable)
    int maxlocks = 4;
    int numlocks = 0;
    RewriteRule **rules = MemoryContextAlloc(rulescxt,
                                           sizeof(RewriteRule *) * maxlocks);

    // Set up scan key for pg_rewrite catalog
    ScanKeyData key;
    ScanKeyInit(&key, Anum_pg_rewrite_ev_class, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));

    // Scan pg_rewrite in rule name order for consistent firing order
    Relation rewrite_desc = table_open(RewriteRelationId, AccessShareLock);
    TupleDesc rewrite_tupdesc = RelationGetDescr(rewrite_desc);
    SysScanDesc rewrite_scan = systable_beginscan(rewrite_desc,
                                                 RewriteRelRulenameIndexId,
                                                 true, NULL, 1, &key);

    HeapTuple rewrite_tuple;
    while (HeapTupleIsValid(rewrite_tuple = systable_getnext(rewrite_scan))) {
        Form_pg_rewrite rewrite_form = (Form_pg_rewrite) GETSTRUCT(rewrite_tuple);

        // Allocate and initialize rule structure
        RewriteRule *rule = MemoryContextAlloc(rulescxt, sizeof(RewriteRule));
        rule->ruleId = rewrite_form->oid;
        rule->event = rewrite_form->ev_type - '0';
        rule->enabled = rewrite_form->ev_enabled;
        rule->isInstead = rewrite_form->is_instead;

        // Extract and parse rule action (potentially TOASTed)
        bool isnull;
        Datum rule_datum = heap_getattr(rewrite_tuple, Anum_pg_rewrite_ev_action,
                                       rewrite_tupdesc, &isnull);
        char *rule_str = TextDatumGetCString(rule_datum);

        MemoryContext oldcxt = MemoryContextSwitchTo(rulescxt);
        rule->actions = (List *) stringToNode(rule_str);
        MemoryContextSwitchTo(oldcxt);
        pfree(rule_str);

        // Extract and parse rule qualification
        rule_datum = heap_getattr(rewrite_tuple, Anum_pg_rewrite_ev_qual,
                                 rewrite_tupdesc, &isnull);
        rule_str = TextDatumGetCString(rule_datum);

        oldcxt = MemoryContextSwitchTo(rulescxt);
        rule->qual = (Node *) stringToNode(rule_str);
        MemoryContextSwitchTo(oldcxt);
        pfree(rule_str);

        // Determine security context for permission checks
        Oid check_as_user;
        if (rule->event == CMD_SELECT &&
            relation->rd_rel->relkind == RELKIND_VIEW &&
            RelationHasSecurityInvoker(relation)) {
            // Security invoker view: check as current user
            check_as_user = InvalidOid;
        } else {
            // Normal case: check as relation owner
            check_as_user = relation->rd_rel->relowner;
        }

        // Set security context in rule trees
        setRuleCheckAsUser((Node *) rule->actions, check_as_user);
        setRuleCheckAsUser(rule->qual, check_as_user);

        // Expand rule array if needed
        if (numlocks >= maxlocks) {
            maxlocks *= 2;
            rules = repalloc(rules, sizeof(RewriteRule *) * maxlocks);
        }
        rules[numlocks++] = rule;
    }

    // Clean up scan
    systable_endscan(rewrite_scan);
    table_close(rewrite_desc, AccessShareLock);

    // Handle case where no rules exist (relhasrules may be outdated)
    if (numlocks == 0) {
        relation->rd_rules = NULL;
        relation->rd_rulescxt = NULL;
        MemoryContextDelete(rulescxt);
        return;
    }

    // Create and install RuleLock structure
    RuleLock *rulelock = MemoryContextAlloc(rulescxt, sizeof(RuleLock));
    rulelock->numLocks = numlocks;
    rulelock->rules = rules;
    relation->rd_rules = rulelock;
}
```