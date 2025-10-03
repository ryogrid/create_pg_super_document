# all_rows_selectable

## Location
[src/backend/utils/adt/selfuncs.c:5618-5800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L5618-L5800)

## Overview
Tests whether the current user has permission to select all rows from a specified relation, including checking for security qualifiers from security barrier views and RLS policies.

## Definition

```c
bool
all_rows_selectable(PlannerInfo *root, Index varno, Bitmapset *varattnos)
```
## Detailed Description
This function determines if a user has the necessary permissions to access all rows from a relation without any security restrictions. It performs comprehensive security checks including:

1. **Permission Verification**: Checks both table-level and column-level SELECT privileges
2. **Security Qualifier Analysis**: Ensures no security barriers from views or Row-Level Security (RLS) policies would restrict access
3. **Inheritance Handling**: For inheritance child relations, it maps attributes to the parent relation and checks permissions against the root parent
4. **View Access**: When accessing through views, it verifies the view owner's permissions on the underlying relation

The function handles complex scenarios like partitioned tables where it must walk up the inheritance hierarchy and map child table attributes to their corresponding parent table attributes.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing query planning context and relation information
- `varno`: Index of the relation in the range table (must be an RTE_RELATION type)
- `*varattnos`: Bitmapset of attribute numbers requiring permission checks, offset by FirstLowInvalidHeapAttributeNumber to handle system attributes; NULL means whole-table access is required
## Dependencies
- Functions called/Symbols referenced:
  - [find_base_rel_noerr](../f/find_base_rel_noerr.md) (relation lookup without error)
  - planner_rt_fetch (range table entry retrieval)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md) (permission information lookup)
  - [bms_next_member](../b/bms_next_member.md) (bitmap set iteration)
  - [bms_add_member](../b/bms_add_member.md) (bitmap set manipulation)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md) (table-level permission checking)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md) (column-level permission checking)
  - [pg_attribute_aclcheck_all](../p/pg_attribute_aclcheck_all.md) (all-columns permission checking)
- Called from (representative examples):
  - [examine_variable](../e/examine_variable.md) (main variable examination function)
  - [examine_simple_variable](../e/examine_simple_variable.md) (simple variable examination)
  - [statext_is_compatible_clause](../s/statext_is_compatible_clause.md) (extended statistics compatibility checking)

## Notes and Other Information
- Returns false if any security qualifiers (securityQuals) are present, indicating restricted access due to security barriers or RLS policies
- For inheritance hierarchies, permissions are checked against the root parent relation, but the function correctly maps child attributes to parent attributes
- The function supports both whole-table access checks (when varattnos is NULL) and specific column access checks
- System attributes (negative attribute numbers) are treated specially and mapped consistently across inheritance hierarchies
- This function is exported for use by other estimation functions that need to determine data access permissions
- The userid determination handles both direct table access and access through views, using the appropriate user context for permission checks

## Simplified Source

```c
bool
all_rows_selectable(PlannerInfo *root, Index varno, Bitmapset *varattnos)
{
    RelOptInfo *rel = find_base_rel_noerr(root, varno);
    RangeTblEntry *rte = planner_rt_fetch(varno, root);
    Oid userid;
    int varattno;

    Assert(rte->rtekind == RTE_RELATION);

    // Determine the user ID for privilege checks (current user or view owner)
    if (rel)
        userid = rel->userid;
    else {
        RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);
        userid = perminfo->checkAsUser;
    }
    if (!OidIsValid(userid))
        userid = GetUserId();

    // Handle inheritance hierarchies - navigate to root parent
    if (root->append_rel_array != NULL) {
        AppendRelInfo *appinfo = root->append_rel_array[varno];

        // Walk up inheritance hierarchy to root parent
        while (appinfo &&
               planner_rt_fetch(appinfo->parent_relid, root)->rtekind == RTE_RELATION) {
            Bitmapset *parent_varattnos = NULL;

            // Map child attributes to parent attributes
            varattno = -1;
            while ((varattno = bms_next_member(varattnos, varattno)) >= 0) {
                AttrNumber attno = varattno + FirstLowInvalidHeapAttributeNumber;
                AttrNumber parent_attno;

                if (attno == InvalidAttrNumber) {
                    // Whole-row reference - map all columns
                    for (attno = 1; attno <= appinfo->num_child_cols; attno++) {
                        parent_attno = appinfo->parent_colnos[attno - 1];
                        if (parent_attno == 0)
                            return false;  // local to child
                        parent_varattnos = bms_add_member(parent_varattnos,
                                         parent_attno - FirstLowInvalidHeapAttributeNumber);
                    }
                } else {
                    // Regular or system attribute
                    if (attno < 0) {
                        parent_attno = attno;  // system attributes same everywhere
                    } else {
                        if (attno > appinfo->num_child_cols)
                            return false;
                        parent_attno = appinfo->parent_colnos[attno - 1];
                        if (parent_attno == 0)
                            return false;  // local to child
                    }
                    parent_varattnos = bms_add_member(parent_varattnos,
                                     parent_attno - FirstLowInvalidHeapAttributeNumber);
                }
            }

            // Continue up the hierarchy
            varno = appinfo->parent_relid;
            varattnos = parent_varattnos;
            appinfo = root->append_rel_array[varno];
        }

        rte = planner_rt_fetch(varno, root);
        Assert(rte->rtekind == RTE_RELATION);
    }

    // Check for security qualifiers (RLS policies, security barrier views)
    if (rte->securityQuals != NIL)
        return false;

    // Check table-level SELECT privilege
    if (pg_class_aclcheck(rte->relid, userid, ACL_SELECT) == ACLCHECK_OK)
        return true;

    if (varattnos == NULL)
        return false;  // whole-table access requested but denied

    // Check column-level privileges
    varattno = -1;
    while ((varattno = bms_next_member(varattnos, varattno)) >= 0) {
        AttrNumber attno = varattno + FirstLowInvalidHeapAttributeNumber;

        if (attno == InvalidAttrNumber) {
            // Whole-row reference - check all columns
            if (pg_attribute_aclcheck_all(rte->relid, userid, ACL_SELECT,
                                         ACLMASK_ALL) != ACLCHECK_OK)
                return false;
        } else {
            // Check specific column
            if (pg_attribute_aclcheck(rte->relid, attno, userid,
                                     ACL_SELECT) != ACLCHECK_OK)
                return false;
        }
    }

    return true;  // All required permissions verified
}
```