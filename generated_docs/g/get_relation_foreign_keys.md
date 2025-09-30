# get_relation_foreign_keys

## Location
[src/backend/optimizer/util/plancat.c:590-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L590-L704)

## Overview
Retrieves foreign key information for a given relation and creates ForeignKeyOptInfo structures for foreign keys that reference other relations in the current query.

## Definition

```c
static void
get_relation_foreign_keys(PlannerInfo *root, RelOptInfo *rel,
						  Relation relation, bool inhparent)
```
## Detailed Description
This static function extracts foreign key constraints from a relation's relcache entry and creates ForeignKeyOptInfo structures for foreign keys that are relevant to the current query. The function focuses on foreign keys that reference other base relations present in the query's range table, as these are the ones that can potentially be used for join optimization and constraint inference.

The function performs several filtering steps:
1. Only processes base relations (not derived relations)
2. Skips single-relation queries where FKs cannot be useful
3. Ignores inheritance parent relations to avoid complex constraint analysis
4. Creates ForeignKeyOptInfo entries only for FKs referencing other RTEs in the query
5. Handles self-joins by creating separate entries for each occurrence of the referenced table

The created ForeignKeyOptInfo structures are added to the root->fkey_list for later use by the query optimizer in join planning and constraint propagation.

## Parameters / Member Variables
- : PlannerInfo structure containing the global planning context and fkey_list
- : RelOptInfo structure representing the relation being analyzed
- : Open Relation structure providing access to cached foreign key information
- : Boolean indicating if this is an inheritance parent (causes early return)

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetFKeyList](../R/RelationGetFKeyList.md)
  - RelationGetRelid
  - makeNode
  - [lappend](../l/lappend.md)
  - memcpy, memset
- Called from (representative examples):
  - [get_relation_info](get_relation_info.md)

## Notes and Other Information
- Only processes base relations in multi-relation queries
- Avoids inheritance parents due to complexity of equivalent constraint detection
- Creates separate ForeignKeyOptInfo entries for each matching RTE in self-joins
- The cached foreign key list belongs to relcache and may disappear during cache flushes
- Initial ForeignKeyOptInfo fields are zeroed and populated later by match_foreign_keys_to_quals()
- Useless ForeignKeyOptInfos (for non-baserels) are filtered out later in the optimization process

## Simplified Source

```c
static void
get_relation_foreign_keys(PlannerInfo *root, RelOptInfo *rel,
                          Relation relation, bool inhparent)
{
    List *rtable = root->parse->rtable;
    List *cachedfkeys;
    ListCell *lc;

    // Only process base relations in multi-relation queries
    if (rel->reloptkind != RELOPT_BASEREL || list_length(rtable) < 2)
        return;

    // Skip inheritance parents (too complex for constraint analysis)
    if (inhparent)
        return;

    // Get cached foreign key list from relcache
    cachedfkeys = RelationGetFKeyList(relation);

    // Process each foreign key constraint
    foreach(lc, cachedfkeys) {
        ForeignKeyCacheInfo *cachedfk = (ForeignKeyCacheInfo *) lfirst(lc);
        Index rti;
        ListCell *lc2;

        // Find RTEs that match the referenced table
        rti = 0;
        foreach(lc2, rtable) {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc2);
            ForeignKeyOptInfo *info;

            rti++;

            // Skip non-matching or inappropriate RTEs
            if (rte->rtekind != RTE_RELATION ||
                rte->relid != cachedfk->confrelid ||
                rte->inh ||                    // Skip inheritance parents
                rti == rel->relid)             // Skip self-references
                continue;

            // Create ForeignKeyOptInfo for this FK relationship
            info = makeNode(ForeignKeyOptInfo);
            info->con_relid = rel->relid;
            info->ref_relid = rti;
            info->nkeys = cachedfk->nkeys;

            // Copy key arrays from cache
            memcpy(info->conkey, cachedfk->conkey, sizeof(info->conkey));
            memcpy(info->confkey, cachedfk->confkey, sizeof(info->confkey));
            memcpy(info->conpfeqop, cachedfk->conpfeqop, sizeof(info->conpfeqop));

            // Initialize fields for later processing
            info->nmatched_ec = 0;
            info->nconst_ec = 0;
            info->nmatched_rcols = 0;
            info->nmatched_ri = 0;
            memset(info->eclass, 0, sizeof(info->eclass));
            memset(info->fk_eclass_member, 0, sizeof(info->fk_eclass_member));
            memset(info->rinfos, 0, sizeof(info->rinfos));

            // Add to global foreign key list
            root->fkey_list = lappend(root->fkey_list, info);
        }
    }
}
```