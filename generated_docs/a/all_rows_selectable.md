# all_rows_selectable

## Location
src/backend/utils/adt/selfuncs.c: 5618 - 5800

## Overview
Tests whether the current user has permission to select all rows from a specified relation, including checking for security qualifiers from security barrier views and RLS policies.

## Definition


## Detailed Description
This function determines if a user has the necessary permissions to access all rows from a relation without any security restrictions. It performs comprehensive security checks including:

1. **Permission Verification**: Checks both table-level and column-level SELECT privileges
2. **Security Qualifier Analysis**: Ensures no security barriers from views or Row-Level Security (RLS) policies would restrict access
3. **Inheritance Handling**: For inheritance child relations, it maps attributes to the parent relation and checks permissions against the root parent
4. **View Access**: When accessing through views, it verifies the view owner's permissions on the underlying relation

The function handles complex scenarios like partitioned tables where it must walk up the inheritance hierarchy and map child table attributes to their corresponding parent table attributes.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and relation information
- : Index of the relation in the range table (must be an RTE_RELATION type)
- : Bitmapset of attribute numbers requiring permission checks, offset by FirstLowInvalidHeapAttributeNumber to handle system attributes; NULL means whole-table access is required

## Dependencies
- Functions called/Symbols referenced:
  - find_base_rel_noerr (relation lookup without error)
  - planner_rt_fetch (range table entry retrieval)
  - getRTEPermissionInfo (permission information lookup)
  - bms_next_member (bitmap set iteration)
  - bms_add_member (bitmap set manipulation)
  - pg_class_aclcheck (table-level permission checking)
  - pg_attribute_aclcheck (column-level permission checking)
  - pg_attribute_aclcheck_all (all-columns permission checking)
- Called from (representative examples):
  - examine_variable (main variable examination function)
  - examine_simple_variable (simple variable examination)
  - statext_is_compatible_clause (extended statistics compatibility checking)

## Notes and Other Information
- Returns false if any security qualifiers (securityQuals) are present, indicating restricted access due to security barriers or RLS policies
- For inheritance hierarchies, permissions are checked against the root parent relation, but the function correctly maps child attributes to parent attributes
- The function supports both whole-table access checks (when varattnos is NULL) and specific column access checks
- System attributes (negative attribute numbers) are treated specially and mapped consistently across inheritance hierarchies
- This function is exported for use by other estimation functions that need to determine data access permissions
- The userid determination handles both direct table access and access through views, using the appropriate user context for permission checks