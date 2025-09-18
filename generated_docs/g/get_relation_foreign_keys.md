# get_relation_foreign_keys

## Location
src/backend/optimizer/util/plancat.c: 590 - 704

## Overview
Retrieves foreign key information for a given relation and creates ForeignKeyOptInfo structures for foreign keys that reference other relations in the current query.

## Definition


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
  - RelationGetFKeyList
  - RelationGetRelid
  - makeNode
  - lappend
  - memcpy, memset
- Called from (representative examples):
  - get_relation_info

## Notes and Other Information
- Only processes base relations in multi-relation queries
- Avoids inheritance parents due to complexity of equivalent constraint detection
- Creates separate ForeignKeyOptInfo entries for each matching RTE in self-joins
- The cached foreign key list belongs to relcache and may disappear during cache flushes
- Initial ForeignKeyOptInfo fields are zeroed and populated later by match_foreign_keys_to_quals()
- Useless ForeignKeyOptInfos (for non-baserels) are filtered out later in the optimization process