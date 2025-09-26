# TryReuseForeignKey

## Location
[src/backend/commands/tablecmds.c:14319-14358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L14319-L14358)

## Overview
TryReuseForeignKey is a subroutine used during column type alteration to extract and preserve the primary-foreign key equality operators from an existing foreign key constraint for potential reuse during constraint revalidation.

## Definition
```c
static void
TryReuseForeignKey(Oid oldId, Constraint *con)
```

## Detailed Description
This function is a helper routine for ATPostAlterTypeParse() that prepares a foreign key constraint for potential reuse during column type alteration operations. It retrieves the primary-foreign key equality operators (conpfeqop) from the system catalog for an existing foreign key constraint and stores them in the Constraint node's old_conpfeqop field. This information is later used by ATAddForeignKeyConstraint() to determine whether revalidation of the constraint can be skipped when the constraint is recreated.

The function performs validation on the conpfeqop array to ensure it's properly formatted as a 1-dimensional OID array without null values, following the same validation logic used in ri_FetchConstraintInfo().

## Parameters
- `oldId`: OID of the existing foreign key constraint
- `con`: Constraint node that will be populated with the old equality operators

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - [lappend_oid](../l/lappend_oid.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - CONSTR_FOREIGN
- Called from:
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md)

## Notes and Other Information
- The function assumes the constraint type is CONSTR_FOREIGN and that old_conpfeqop has not been previously set
- Error handling includes cache lookup validation and array format verification
- The extracted operator OIDs are stored as a List in the old_conpfeqop field
- This optimization allows foreign key constraint revalidation to be skipped when operators remain compatible after column type changes
- The validation logic mirrors that used in ri_FetchConstraintInfo() for consistency