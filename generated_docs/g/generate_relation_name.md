# generate_relation_name

## Location
[src/backend/utils/adt/ruleutils.c:12823-12882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12823-L12882)

## Overview
Computes the properly qualified and quoted name to display for a relation specified by OID, handling namespace conflicts and visibility rules.

## Definition

```c
static char *
generate_relation_name(Oid relid, List *namespaces)
```
## Detailed Description
This function generates a display-ready relation name by looking up the relation in the system catalog and applying appropriate qualification rules. It handles several important scenarios: (1) checks for conflicts with Common Table Expression (CTE) names in the provided namespace list, (2) determines if the relation needs schema qualification based on search path visibility, and (3) applies proper SQL identifier quoting. The function prioritizes avoiding name conflicts over brevity, ensuring generated SQL is unambiguous.

## Parameters / Member Variables
- `relid`: The OID of the relation to generate a name for
- `namespaces`: List of deparse_namespace nodes representing the current parsing context; used to check for CTE name conflicts

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - NameStr
  - [RelationIsVisible](../R/RelationIsVisible.md)
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [pg_get_constraintdef_worker](../p/pg_get_constraintdef_worker.md)
  - [make_ruledef](../m/make_ruledef.md)
  - [get_insert_query_def](get_insert_query_def.md)
  - [get_update_query_def](get_update_query_def.md)
  - [get_from_clause_item](get_from_clause_item.md)

## Notes and Other Information
- This is a static function local to ruleutils.c
- Implements intelligent qualification logic: qualifies names when they conflict with CTEs or are not visible in the search path
- Essential for PostgreSQL's rule deparsing and SQL generation systems
- Uses system cache lookups for efficient relation metadata retrieval
- Part of the broader ruleutils.c infrastructure for generating readable SQL from internal representations
- The returned string is palloc'd and must be freed by the caller

## Simplified Source

```c
static char *generate_relation_name(Oid relid, List *namespaces) {
    HeapTuple tp;
    Form_pg_class reltup;
    bool need_qual;
    ListCell *nslist;
    char *relname;
    char *nspname;
    char *result;

    // Look up relation information in system cache
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for relation %u", relid);

    reltup = (Form_pg_class) GETSTRUCT(tp);
    relname = NameStr(reltup->relname);

    // Check for conflicting CTE names in all namespaces
    need_qual = false;
    foreach(nslist, namespaces) {
        deparse_namespace *dpns = (deparse_namespace *) lfirst(nslist);
        ListCell *ctlist;

        foreach(ctlist, dpns->ctes) {
            CommonTableExpr *cte = (CommonTableExpr *) lfirst(ctlist);

            if (strcmp(cte->ctename, relname) == 0) {
                need_qual = true;
                break;
            }
        }
        if (need_qual)
            break;
    }

    // If no CTE conflict, check if relation is visible in search path
    if (!need_qual)
        need_qual = !RelationIsVisible(relid);

    // Get schema name if qualification is needed
    if (need_qual)
        nspname = get_namespace_name_or_temp(reltup->relnamespace);
    else
        nspname = NULL;

    // Create properly quoted and qualified identifier
    result = quote_qualified_identifier(nspname, relname);

    ReleaseSysCache(tp);

    return result;
}
```