# checkViewColumns

## Location
src/backend/commands/view.c: 267 - 331

## Overview
checkViewColumns verifies that the columns of a proposed new view definition are compatible with the columns of an existing view during view replacement operations.

## Definition


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