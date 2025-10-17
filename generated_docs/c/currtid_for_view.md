# currtid_for_view

## Location
[src/backend/utils/adt/tid.c:336-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L336-L407)

## Overview
A specialized function that handles current tuple identifier (CTID) operations for views by analyzing their rule definitions and delegating to the underlying base relations.

## Definition
```c
static ItemPointer currtid_for_view(Relation viewrel, ItemPointer tid)
```

## Detailed Description
The `currtid_for_view` function handles CTID operations specifically for PostgreSQL views. Since views are virtual tables that don't store data directly, this function must analyze the view's definition to find the underlying base relation that actually contains the tuple data.

The function works by:
1. Examining the view's tuple descriptor to locate a CTID column
2. Validating that the CTID column has the correct TID type
3. Analyzing the view's rewrite rules to find the SELECT rule
4. Examining the target list of the SELECT query to identify which base relation the CTID refers to
5. Opening the base relation and delegating the actual CTID lookup to `currtid_internal`

The implementation includes several validation checks to ensure the view is properly structured with exactly one SELECT rule and a valid CTID column that maps to a base relation's system CTID column.

## Parameters / Member Variables
- `viewrel`: The view relation for which to handle the CTID operation
- `tid`: Pointer to the tuple identifier to look up in the underlying base relation

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr
  - TupleDescAttr
  - NameStr
  - elog
  - [list_length](../l/list_length.md)
  - linitial
  - [get_tle_by_resno](../g/get_tle_by_resno.md)
  - IsA
  - IS_SPECIAL_VARNO
  - rt_fetch
  - [table_open](../t/table_open.md)
  - [currtid_internal](currtid_internal.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [currtid_internal](currtid_internal.md)

## Notes and Other Information
- This is a static function with internal linkage, used only within the TID utilities module
- The function performs extensive validation to ensure the view is compatible with CTID operations
- Views must have exactly one SELECT rule to be supported
- The CTID column in the view must map directly to a base relation's system CTID column (SelfItemPointerAttributeNumber)
- The function uses AccessShareLock when opening the base relation to ensure safe concurrent access
- Error handling is comprehensive, with specific error messages for various failure conditions such as missing CTID columns, invalid CTID types, missing rules, or unsupported view structures
- The implementation supports the PostgreSQL rule system and query rewriting mechanism for views

## Simplified Source

```c
static ItemPointer
currtid_for_view(Relation viewrel, ItemPointer tid)
{
    TupleDesc att = RelationGetDescr(viewrel);
    int natts = att->natts;
    int tididx = -1;

    // Find the CTID column in the view
    for (int i = 0; i < natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(att, i);
        if (strcmp(NameStr(attr->attname), "ctid") == 0) {
            if (attr->atttypid != TIDOID)
                elog(ERROR, "ctid isn't of type TID");
            tididx = i;
            break;
        }
    }
    if (tididx < 0)
        elog(ERROR, "currtid cannot handle views with no CTID");

    // Process view rules to find the base relation
    RuleLock *rulelock = viewrel->rd_rules;
    if (!rulelock)
        elog(ERROR, "the view has no rules");

    for (int i = 0; i < rulelock->numLocks; i++) {
        RewriteRule *rewrite = rulelock->rules[i];
        if (rewrite->event == CMD_SELECT) {
            // Analyze the SELECT rule's target list
            Query *query = (Query *) linitial(rewrite->actions);
            TargetEntry *tle = get_tle_by_resno(query->targetList, tididx + 1);

            if (tle && tle->expr && IsA(tle->expr, Var)) {
                Var *var = (Var *) tle->expr;
                if (!IS_SPECIAL_VARNO(var->varno) &&
                    var->varattno == SelfItemPointerAttributeNumber) {
                    // Found base relation, delegate to it
                    RangeTblEntry *rte = rt_fetch(var->varno, query->rtable);
                    if (rte) {
                        Relation rel = table_open(rte->relid, AccessShareLock);
                        ItemPointer result = currtid_internal(rel, tid);
                        table_close(rel, AccessShareLock);
                        return result;
                    }
                }
            }
            break;
        }
    }
    elog(ERROR, "currtid cannot handle this view");
    return NULL;
}
```