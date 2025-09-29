# addRTEPermissionInfo

## Location
[src/backend/parser/parse_relation.c:3874-3902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3874-L3902)

## Overview
This function creates an RTEPermissionInfo structure for a given RangeTblEntry and adds it to the provided list, establishing the connection between range table entries and their permission information.

## Definition
```c
RTEPermissionInfo *addRTEPermissionInfo(List **rteperminfos, RangeTblEntry *rte)
```

## Detailed Description
`addRTEPermissionInfo` creates and initializes an RTEPermissionInfo node for a given range table entry and adds it to the permission info list. This function is essential for PostgreSQL's permission checking system, as it establishes the mapping between relations in the query and their associated permission requirements. The function creates a new RTEPermissionInfo node, copies the relation ID and inheritance flag from the RTE, appends it to the permission info list, and sets the 1-based index in the RTE that points back to this permission info. This design separates permission information from the core range table structure while maintaining efficient bidirectional references.

## Parameters / Member Variables
- `rteperminfos`: Pointer to a List pointer that will be updated to include the new RTEPermissionInfo
- `rte`: Pointer to the RangeTblEntry for which permission information is being created

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [lappend](../l/lappend.md)
  - [list_length](../l/list_length.md)
  - OidIsValid (via Assert)
- Data structures used:
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md)
  - [RangeTblEntry](../R/RangeTblEntry.md)
  - [List](../L/List.md)
- Called from (representative examples):
  - [addRangeTableEntry](addRangeTableEntry.md) (src/backend/parser/parse_relation.c:1523)
  - [addRangeTableEntryForRelation](addRangeTableEntryForRelation.md) (src/backend/parser/parse_relation.c:1608)
  - [add_rte_to_flat_rtable](add_rte_to_flat_rtable.md) (src/backend/optimizer/plan/setrefs.c:599)
  - [plan_cluster_use_sort](../p/plan_cluster_use_sort.md) (src/backend/optimizer/plan/planner.c:6781)

## Notes and Other Information
- The function requires that the RTE has a valid relation ID (checked via Assert)
- The function expects that the RTE does not already have a permission info index set (perminfoindex == 0)
- The permission info index stored in the RTE is 1-based, not 0-based
- Only basic information (relid and inh flag) is copied initially; other permission details are populated as needed later
- This function is part of PostgreSQL's access control infrastructure and is declared in src/include/parser/parse_relation.h
- The returned RTEPermissionInfo pointer allows immediate access to the newly created permission info structure

## Simplified Source

```c
RTEPermissionInfo *
addRTEPermissionInfo(List **rteperminfos, RangeTblEntry *rte)
{
    RTEPermissionInfo *perminfo;

    Assert(OidIsValid(rte->relid));
    Assert(rte->perminfoindex == 0);

    // Create new permission info node
    perminfo = makeNode(RTEPermissionInfo);
    perminfo->relid = rte->relid;
    perminfo->inh = rte->inh;

    // Add to permission info list
    *rteperminfos = lappend(*rteperminfos, perminfo);

    // Set 1-based index in RTE
    rte->perminfoindex = list_length(*rteperminfos);

    return perminfo;
}
```