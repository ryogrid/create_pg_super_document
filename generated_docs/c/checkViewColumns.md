# checkViewColumns

## Location
[src/backend/commands/view.c:267-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/view.c#L267-L331)

## Overview
checkViewColumns verifies that the columns of a proposed new view definition are compatible with the columns of an existing view during view replacement operations.

## Definition

```c
static void
checkViewColumns(TupleDesc newdesc, TupleDesc olddesc)
```
## Detailed Description
checkViewColumns performs strict compatibility checking between old and new view column definitions during CREATE OR REPLACE VIEW operations. The function ensures that view replacement maintains backward compatibility by enforcing the following rules:

1. **Column Count**: The new view must have at least as many columns as the old view (adding columns is allowed, but dropping is not)
2. **Column Names**: Existing column names cannot be changed
3. **Data Types**: Column data types and type modifiers cannot be changed
4. **Collations**: Column collations cannot be changed
5. **Dropped Status**: The dropped/active status of columns must remain consistent

The function is similar to equalRowTypes() but provides specific error messages for view-related violations. It allows the new view to have additional columns beyond those in the original view, supporting view expansion scenarios.

The strict enforcement of type, typmod, and collation immutability is critical because these properties may be embedded in Vars of other views or rules that reference this view, and changing them would break those dependent objects.

## Parameters / Member Variables
- : TupleDesc representing the column structure of the proposed new view
- : TupleDesc representing the column structure of the existing view being replaced

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (macro for accessing tuple descriptor attributes)
  - NameStr (macro for converting Name to string)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
  - [get_collation_name](../g/get_collation_name.md)
  - ereport, errcode, errmsg, errhint (error reporting)

- Called from:
  - [DefineVirtualRelation](../D/DefineVirtualRelation.md)

## Notes and Other Information
- The function allows new views to have more columns than old views, but not fewer
- Column constraints are ignored during comparison since new views cannot have constraints and existing defaults are preserved
- The error messages provide helpful hints, such as suggesting ALTER VIEW ... RENAME COLUMN for name changes
- Type and collation changes are prohibited because they could break dependent views and rules that reference the view
- The XXX comment indicates that the "cannot drop columns" message may not be entirely accurate, but DROP COLUMN is not supported on views anyway

## Simplified Source

```c
static void
checkViewColumns(TupleDesc newdesc, TupleDesc olddesc)
{
    // Check that new view has at least as many columns as old
    if (newdesc->natts < olddesc->natts)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                       errmsg("cannot drop columns from view")));

    // Validate each existing column for compatibility
    for (int i = 0; i < olddesc->natts; i++) {
        Form_pg_attribute newattr = TupleDescAttr(newdesc, i);
        Form_pg_attribute oldattr = TupleDescAttr(olddesc, i);

        // Check dropped status consistency
        if (newattr->attisdropped != oldattr->attisdropped)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("cannot drop columns from view")));

        // Check column name compatibility
        if (strcmp(NameStr(newattr->attname), NameStr(oldattr->attname)) != 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("cannot change name of view column \"%s\" to \"%s\"",
                                 NameStr(oldattr->attname), NameStr(newattr->attname)),
                           errhint("Use ALTER VIEW ... RENAME COLUMN ... to change name of view column instead.")));

        // Check data type and type modifier compatibility
        if (newattr->atttypid != oldattr->atttypid || newattr->atttypmod != oldattr->atttypmod)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("cannot change data type of view column \"%s\" from %s to %s",
                                 NameStr(oldattr->attname),
                                 format_type_with_typemod(oldattr->atttypid, oldattr->atttypmod),
                                 format_type_with_typemod(newattr->atttypid, newattr->atttypmod))));

        // Check collation compatibility
        if (newattr->attcollation != oldattr->attcollation)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("cannot change collation of view column \"%s\" from \"%s\" to \"%s\"",
                                 NameStr(oldattr->attname),
                                 get_collation_name(oldattr->attcollation),
                                 get_collation_name(newattr->attcollation))));
    }

    // Note: Constraint fields are ignored - new views can't have constraints
    // and existing defaults are preserved
}
```