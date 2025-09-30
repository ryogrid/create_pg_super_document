# expand_single_inheritance_child

## Location
[src/backend/optimizer/util/inherit.c:461-655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/inherit.c#L461-L655)

## Overview
Builds a RangeTblEntry and AppendRelInfo for a single child relation in an inheritance hierarchy, along with optional PlanRowMark for row locking support.

## Definition

```c
structure of the parent RTE has to be
	 * translated to match the child table's column ordering, which we do
	 * below, so a "flat" copy is sufficient to start with.
	 */
	childrte = makeNode(RangeTblEntry);
```
## Detailed Description
This static function creates the necessary planner data structures for a single child relation in an inheritance or partitioning hierarchy. Key operations include:

1. **RTE Creation**: Creates a new RangeTblEntry by copying most fields from the parent RTE but updating relation-specific fields (OID, relkind, inh flag). Sets securityQuals to empty to ensure parent RLS conditions apply uniformly.

2. **AppendRelInfo Construction**: Creates an AppendRelInfo structure using make_append_rel_info to establish the parent-child relationship and column mapping.

3. **Column Alias Management**: Constructs proper column aliases for the child relation by mapping parent column names through the AppendRelInfo, ensuring EXPLAIN output shows correct column names.

4. **PlanRowMark Setup**: If row locking is required, creates a PlanRowMark for the child with appropriate mark type based on the child's relation kind.

5. **Result Relation Handling**: For UPDATE/DELETE/MERGE operations, adds the child to result relation sets and generates necessary row identity columns including tableoid.

The function supports both traditional inheritance and modern partitioning, with partitioned children marked for further expansion.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : RangeTblEntry of the immediate parent relation
- : Index of parent RTE in the range table
- : Open Relation structure for the parent
- : PlanRowMark from the top-level parent for row locking
- : Open Relation structure for the child being processed
- : Output parameter for the created child RangeTblEntry
- : Output parameter for the child's range table index

## Dependencies
- Functions called/Symbols referenced:
  - [make_append_rel_info](../m/make_append_rel_info.md)
  - [makeAlias](../m/makeAlias.md), makeString, makeVar
  - [select_rowmark_type](../s/select_rowmark_type.md)
  - [add_row_identity_var](../a/add_row_identity_var.md)
  - [add_row_identity_columns](../a/add_row_identity_columns.md)
  - copyObject
  - [bms_is_member](../b/bms_is_member.md), bms_add_member
- Called from (representative examples):
  - [expand_inherited_rtentry](expand_inherited_rtentry.md)
  - [expand_partitioned_rtentry](expand_partitioned_rtentry.md)

## Notes and Other Information
- Creates a hierarchical structure where each partitioned descendant acts as parent of its immediate partitions (differs from older flattened approach)
- PlanRowMarks retain the top-parent's RTI while accumulating mark types from all descendants
- Child permissions are handled through the parent - no separate permission checking for child RTEs
- For partitioned children, the inh flag is set to true to trigger further expansion
- Table aliases are duplicated from parent; ruleutils.c handles uniqueness during plan printing
- Row identity columns (tableoid) are automatically added for child target relations in DML operations

## Simplified Source

```c
static void
expand_single_inheritance_child(PlannerInfo *root, RangeTblEntry *parentrte,
                                Index parentRTindex, Relation parentrel,
                                PlanRowMark *top_parentrc, Relation childrel,
                                RangeTblEntry **childrte_p,
                                Index *childRTindex_p)
{
    Query *parse = root->parse;
    Oid childOID = RelationGetRelid(childrel);
    RangeTblEntry *childrte;
    Index childRTindex;
    AppendRelInfo *appinfo;

    // Create child RTE by copying parent RTE with child-specific updates
    childrte = makeNode(RangeTblEntry);
    memcpy(childrte, parentrte, sizeof(RangeTblEntry));

    childrte->relid = childOID;
    childrte->relkind = childrel->rd_rel->relkind;
    childrte->inh = (childrte->relkind == RELKIND_PARTITIONED_TABLE);
    childrte->securityQuals = NIL;  // Use parent's RLS conditions
    childrte->perminfoindex = 0;    // No separate permissions for child

    // Add child RTE to range table
    parse->rtable = lappend(parse->rtable, childrte);
    childRTindex = list_length(parse->rtable);
    *childrte_p = childrte;
    *childRTindex_p = childRTindex;

    // Create AppendRelInfo for parent-child relationship
    appinfo = make_append_rel_info(parentrel, childrel, parentRTindex, childRTindex);
    root->append_rel_list = lappend(root->append_rel_list, appinfo);

    // Set up column aliases for proper EXPLAIN output
    TupleDesc child_tupdesc = RelationGetDescr(childrel);
    List *parent_colnames = parentrte->eref->colnames;
    List *child_colnames = NIL;

    for (int cattno = 0; cattno < child_tupdesc->natts; cattno++) {
        Form_pg_attribute att = TupleDescAttr(child_tupdesc, cattno);
        const char *attname;

        if (att->attisdropped) {
            attname = "";
        } else if (appinfo->parent_colnos[cattno] > 0 &&
                   appinfo->parent_colnos[cattno] <= list_length(parent_colnames)) {
            // Use parent's column name
            attname = strVal(list_nth(parent_colnames, appinfo->parent_colnos[cattno] - 1));
        } else {
            // Use child's actual column name
            attname = NameStr(att->attname);
        }
        child_colnames = lappend(child_colnames, makeString(pstrdup(attname)));
    }

    childrte->alias = childrte->eref = makeAlias(parentrte->eref->aliasname, child_colnames);

    // Store in planner arrays
    root->simple_rte_array[childRTindex] = childrte;
    root->append_rel_array[childRTindex] = appinfo;

    // Create PlanRowMark for row locking if needed
    if (top_parentrc) {
        PlanRowMark *childrc = makeNode(PlanRowMark);
        childrc->rti = childRTindex;
        childrc->prti = top_parentrc->rti;
        childrc->rowmarkId = top_parentrc->rowmarkId;
        childrc->markType = select_rowmark_type(childrte, top_parentrc->strength);
        childrc->allMarkTypes = (1 << childrc->markType);
        childrc->strength = top_parentrc->strength;
        childrc->waitPolicy = top_parentrc->waitPolicy;
        childrc->isParent = (childrte->relkind == RELKIND_PARTITIONED_TABLE);

        top_parentrc->allMarkTypes |= childrc->allMarkTypes;
        root->rowMarks = lappend(root->rowMarks, childrc);
    }

    // Handle result relations for DML operations
    if (bms_is_member(parentRTindex, root->all_result_relids)) {
        root->all_result_relids = bms_add_member(root->all_result_relids, childRTindex);

        // Add row identity info for leaf relations
        if (childrte->relkind != RELKIND_PARTITIONED_TABLE) {
            root->leaf_result_relids = bms_add_member(root->leaf_result_relids, childRTindex);

            // Add tableoid column for multi-table DML
            Var *rrvar = makeVar(childRTindex, TableOidAttributeNumber, OIDOID, -1, InvalidOid, 0);
            add_row_identity_var(root, rrvar, childRTindex, "tableoid");
            add_row_identity_columns(root, childRTindex, childrte, childrel);
        }
    }
}
```