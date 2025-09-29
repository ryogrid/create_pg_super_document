# set_rtable_names

## Location
[src/backend/utils/adt/ruleutils.c:3828-3972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3828-L3972)

## Overview
Selects unique RTE (Range Table Entry) aliases to be used when printing a query, ensuring name uniqueness across current and parent namespaces.

## Definition
static void set_rtable_names(deparse_namespace *dpns, List *parent_namespaces, Bitmapset *rels_used)

## Detailed Description
This function fills in the rtable_names list within a deparse_namespace structure, creating a one-to-one mapping with the rtable list. Each RTE name is guaranteed to be unique among those in the new namespace plus any ancestor namespaces. The function uses a hash table for efficient O(N) performance instead of O(N^2). When name conflicts occur, it appends numeric suffixes (_1, _2, etc.) to create unique names, and handles name length constraints by truncating if necessary while preserving the numeric suffix.

## Parameters / Member Variables
- dpns: Pointer to deparse_namespace structure to be populated with table names
- parent_namespaces: List of ancestor namespace contexts to avoid name conflicts with
- rels_used: Bitmapset indicating which RTE indexes should be given aliases (can be NULL to process all)

## Dependencies
- Functions called/Symbols referenced:
  - [HASHCTL](../H/HASHCTL.md)
  - [HTAB](../H/HTAB.md)
  - [NameHashEntry](../N/NameHashEntry.md)
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [hash_destroy](../h/hash_destroy.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [pg_mbcliplen](../p/pg_mbcliplen.md)
- Called from (representative examples):
  - [select_rtable_names_for_explain](select_rtable_names_for_explain.md)
  - [set_deparse_for_query](set_deparse_for_query.md)
  - [deparse_context_for](../d/deparse_context_for.md)
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md)

## Notes and Other Information
- This function is static and only concerned with relation names, not column names
- Uses a hash table with NAMEDATALEN key size for efficient name lookup and conflict resolution
- Preloads the hash table with names from parent namespaces to avoid conflicts
- Handles various RTE kinds: user-defined aliases, relation names, joins, and parser-assigned names
- For unnamed joins, assigns NULL as the refname
- Includes interrupt checking for potentially long operations
- Memory management includes proper cleanup of the hash table at function end

## Simplified Source

```c
static void set_rtable_names(deparse_namespace *dpns, List *parent_namespaces,
                            Bitmapset *rels_used) {
    HASHCTL hash_ctl;
    HTAB *names_hash;
    NameHashEntry *hentry;
    bool found;
    int rtindex;
    ListCell *lc;

    dpns->rtable_names = NIL;

    // Nothing to do if empty rtable
    if (dpns->rtable == NIL)
        return;

    // Create hash table for O(N) name uniqueness checking
    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.entrysize = sizeof(NameHashEntry);
    hash_ctl.hcxt = CurrentMemoryContext;
    names_hash = hash_create("set_rtable_names names",
                            list_length(dpns->rtable),
                            &hash_ctl,
                            HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

    // Preload hash with names from parent namespaces
    foreach(lc, parent_namespaces) {
        deparse_namespace *olddpns = (deparse_namespace *) lfirst(lc);
        ListCell *lc2;

        foreach(lc2, olddpns->rtable_names) {
            char *oldname = (char *) lfirst(lc2);

            if (oldname == NULL)
                continue;

            hentry = (NameHashEntry *) hash_search(names_hash, oldname,
                                                  HASH_ENTER, &found);
            hentry->counter = 0;
        }
    }

    // Process each RTE in the rtable
    rtindex = 1;
    foreach(lc, dpns->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        char *refname;

        CHECK_FOR_INTERRUPTS();

        // Skip unreferenced RTEs if rels_used is specified
        if (rels_used && !bms_is_member(rtindex, rels_used)) {
            refname = NULL;
        }
        // Prefer user-defined alias
        else if (rte->alias) {
            refname = rte->alias->aliasname;
        }
        // Use relation name for relations
        else if (rte->rtekind == RTE_RELATION) {
            refname = get_rel_name(rte->relid);
        }
        // Unnamed joins have no refname
        else if (rte->rtekind == RTE_JOIN) {
            refname = NULL;
        }
        // Use parser-assigned name for other cases
        else {
            refname = rte->eref->aliasname;
        }

        // Make name unique if needed
        if (refname) {
            hentry = (NameHashEntry *) hash_search(names_hash, refname,
                                                  HASH_ENTER, &found);

            if (found) {
                // Name already exists, create unique variant
                int refnamelen = strlen(refname);
                char *modname = (char *) palloc(refnamelen + 16);
                NameHashEntry *hentry2;

                do {
                    hentry->counter++;
                    for (;;) {
                        memcpy(modname, refname, refnamelen);
                        sprintf(modname + refnamelen, "_%d", hentry->counter);
                        if (strlen(modname) < NAMEDATALEN)
                            break;
                        // Truncate original name to fit suffix
                        refnamelen = pg_mbcliplen(refname, refnamelen,
                                                 refnamelen - 1);
                    }
                    hentry2 = (NameHashEntry *) hash_search(names_hash, modname,
                                                           HASH_ENTER, &found);
                } while (found);

                hentry2->counter = 0;
                refname = modname;
            } else {
                // First use of this name
                hentry->counter = 0;
            }
        }

        dpns->rtable_names = lappend(dpns->rtable_names, refname);
        rtindex++;
    }

    hash_destroy(names_hash);
}
```