# adjust_inherited_attnums_multilevel

## Location
[src/backend/optimizer/util/appendinfo.c:662-689](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L662-L689)

## Overview
Translates attribute numbers through multiple inheritance levels, handling complex inheritance hierarchies where a child relation may be separated from the target parent by intermediate inheritance levels.

## Definition
```c
List *adjust_inherited_attnums_multilevel(PlannerInfo *root, List *attnums, Index child_relid, Index top_parent_relid)
```

## Detailed Description
This function extends the basic attribute number translation functionality to handle multi-level inheritance hierarchies. It recursively traverses the inheritance chain from a child relation up to a specified top parent relation, applying attribute number translations at each level. The function uses the PlannerInfo's append_rel_array to navigate the inheritance hierarchy and locate the appropriate AppendRelInfo structures for each level.

The recursive approach ensures that all intermediate inheritance mappings are properly applied, making it possible to translate attribute numbers across arbitrarily deep inheritance hierarchies. This is essential for complex partitioned table structures where multiple levels of inheritance may exist.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning context and append_rel_array
- `attnums`: List of integer attribute numbers to be translated from the top parent perspective
- `child_relid`: Relation ID of the child relation (target of translation)
- `top_parent_relid`: Relation ID of the top-level parent relation (source of translation)

## Dependencies
- Functions called/Symbols referenced:
  - [AppendRelInfo](../A/AppendRelInfo.md) (structure type)
  - [adjust_inherited_attnums_multilevel](adjust_inherited_attnums_multilevel.md) (recursive self-call)
  - [adjust_inherited_attnums](adjust_inherited_attnums.md) (performs single-level attribute translation)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)
  - [adjust_inherited_attnums_multilevel](adjust_inherited_attnums_multilevel.md) (recursive calls)
  - [get_translated_update_targetlist](../g/get_translated_update_targetlist.md)

## Notes and Other Information
- Implements recursive traversal of inheritance hierarchies to handle arbitrarily deep nesting
- Includes error checking to ensure the child relation exists in the append_rel_array
- Essential for PostgreSQL's handling of complex partitioned table hierarchies
- Uses the PlannerInfo's append_rel_array as the authoritative source for inheritance relationships
- Properly handles the bottom-up translation process from child to top parent

## Simplified Source

```c
List *
adjust_inherited_attnums_multilevel(PlannerInfo *root, List *attnums,
                                    Index child_relid, Index top_parent_relid)
{
    AppendRelInfo *appinfo = root->append_rel_array[child_relid];

    if (!appinfo)
        elog(ERROR, "child rel %d not found in append_rel_array", child_relid);

    // Recurse if immediate parent is not the top parent
    if (appinfo->parent_relid != top_parent_relid)
        attnums = adjust_inherited_attnums_multilevel(root, attnums,
                                                      appinfo->parent_relid,
                                                      top_parent_relid);

    // Translate for this child level
    return adjust_inherited_attnums(attnums, appinfo);
}
```