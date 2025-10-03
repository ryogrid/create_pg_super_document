# adjust_child_relids

## Location
[src/backend/optimizer/util/appendinfo.c:554-587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L554-L587)

## Overview
Substitutes child relation IDs for parent relation IDs in a Relids set based on the provided AppendRelInfo mappings.

## Definition

```c
Relids
adjust_child_relids(Relids relids, int nappinfos, AppendRelInfo **appinfos)
```
## Detailed Description
This function performs relation ID substitution for query planning optimization, specifically for handling inheritance and partitioning scenarios. It iterates through an array of AppendRelInfo structures and replaces any parent relation IDs found in the input Relids set with their corresponding child relation IDs. The function is designed to be efficient by only creating a copy of the input set when modifications are actually needed.

The substitution process removes the parent relation ID from the set and adds the corresponding child relation ID. This is essential for query optimization when dealing with partitioned tables or inheritance hierarchies where the planner needs to work with specific child relations rather than abstract parent relations.

## Parameters / Member Variables
- `relids`: Input Relids set containing relation IDs to be processed
- `nappinfos`: Number of AppendRelInfo structures in the appinfos array
- `**appinfos`: Array of AppendRelInfo pointers containing parent-to-child relation ID mappings
## Dependencies
- Functions called/Symbols referenced:
  - [AppendRelInfo](../A/AppendRelInfo.md) (structure type)
  - [bms_is_member](../b/bms_is_member.md) (checks if relation ID exists in set)
  - [bms_copy](../b/bms_copy.md) (creates copy of bitmap set)
  - [bms_del_member](../b/bms_del_member.md) (removes relation ID from set)
  - [bms_add_member](../b/bms_add_member.md) (adds relation ID to set)
- Called from (representative examples):
  - [try_partitionwise_join](../t/try_partitionwise_join.md)
  - [build_child_join_sjinfo](../b/build_child_join_sjinfo.md)
  - [adjust_appendrel_attrs_mutator](adjust_appendrel_attrs_mutator.md)
  - [adjust_child_relids_multilevel](adjust_child_relids_multilevel.md)
  - build_child_join_rel

## Notes and Other Information
- Returns the original input set unchanged if no substitutions are needed, avoiding unnecessary memory allocation
- Only creates a modified copy when actual changes are required, optimizing memory usage
- Part of PostgreSQL's append relation handling infrastructure for inheritance and partitioning
- Used extensively in join planning and relation processing for partitioned tables

## Simplified Source

```c
// Simplified version of adjust_child_relids
Relids
adjust_child_relids(Relids relids, int nappinfos, AppendRelInfo **appinfos)
{
    Bitmapset *result = NULL;
    int cnt;

    // Check each AppendRelInfo for parent->child substitutions
    for (cnt = 0; cnt < nappinfos; cnt++)
    {
        AppendRelInfo *appinfo = appinfos[cnt];

        // If parent relation is in the set, substitute with child
        if (bms_is_member(appinfo->parent_relid, relids))
        {
            // Create copy only when we need to make changes
            if (!result)
                result = bms_copy(relids);

            // Remove parent, add child
            result = bms_del_member(result, appinfo->parent_relid);
            result = bms_add_member(result, appinfo->child_relid);
        }
    }

    // Return modified copy if changes were made, otherwise original set
    return result ? result : relids;
}
```