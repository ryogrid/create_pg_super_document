# checkFkeyPermissions

## Location
[src/backend/commands/tablecmds.c:12212-12240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12212-L12240)

## Overview
Validates that the current user has sufficient REFERENCES privileges on the referenced table and columns for creating a foreign key constraint.

## Definition

```c
structure;
```
## Detailed Description
This function performs permission checks to ensure the current user has the necessary REFERENCES privileges to create a foreign key constraint that references specific columns in a target table. It implements a two-tier permission model: first checking for table-level REFERENCES permission (which grants access to all columns), and if that fails, checking for column-level REFERENCES permission on each individually specified column. The function assumes that ownership of the referencing table has already been verified earlier in the process.

The permission verification follows PostgreSQL's standard access control model where REFERENCES privilege can be granted either at the table level (covering all columns) or at individual column levels for more granular control.

## Parameters / Member Variables
- : The referenced relation (table) that the foreign key will point to
- : Array of attribute numbers representing the specific columns being referenced
- : Number of attributes (columns) in the foreign key reference

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - ACL_REFERENCES
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)

## Notes and Other Information
- Assumes the user already owns the referencing table (checked elsewhere)
- Uses efficient short-circuit evaluation: table-level permission check first, then column-level if needed
- Raises appropriate access control errors if insufficient privileges are found
- Part of the security validation process during foreign key constraint creation
- Follows PostgreSQL's hierarchical permission model for database objects

## Simplified Source

```c
static void
checkFkeyPermissions(Relation rel, int16 *attnums, int natts)
{
    Oid roleid = GetUserId();
    AclResult aclresult;

    // Check table-level REFERENCES permission first
    aclresult = pg_class_aclcheck(RelationGetRelid(rel), roleid, ACL_REFERENCES);
    if (aclresult == ACLCHECK_OK)
        return;

    // If no table-level permission, check each column individually
    for (int i = 0; i < natts; i++)
    {
        aclresult = pg_attribute_aclcheck(RelationGetRelid(rel), attnums[i],
                                         roleid, ACL_REFERENCES);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, get_relkind_objtype(rel->rd_rel->relkind),
                          RelationGetRelationName(rel));
    }
}
```