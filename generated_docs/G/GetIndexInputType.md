# GetIndexInputType

## Location
src/backend/access/spgist/spgutils.c: 115 - 159

## Overview
GetIndexInputType determines the nominal input data type for an index column, preferring the opclass's opcintype or falling back to the base type of the heap column or expression.

## Definition


## Detailed Description
This function determines the appropriate input data type for a given index column by implementing a preference hierarchy. It first checks the opclass's opcintype, and if that's a polymorphic type, it examines the actual input type from either a simple heap column or an index expression. The function ensures that non-polymorphic opclasses don't receive information about binary-compatible types (e.g., preferring "text" over "varchar"), and it flattens domain types when consulting actual input types.

The function handles both simple index columns (referencing heap table columns) and expression-based index columns. For expression columns, it walks through the cached index expressions to find the appropriate expression and determine its type.

## Parameters / Member Variables
- : The relation representing the index
- : The column number in the index (1-based) to get the input type for

## Dependencies
- Functions called/Symbols referenced:
  - IsPolymorphicType (check if type is polymorphic)
  - get_atttype (get attribute type from relation)
  - getBaseType (flatten domain types to base types)
  - RelationGetIndexExpressions (get index expressions if not cached)
  - list_head (get first element of list)
  - lnext (get next element in list)
  - exprType (get type of expression node)
- Called from (representative examples):
  - spgGetCache (at src/backend/access/spgist/spgutils.c:204)

## Notes and Other Information
- Located in src/backend/access/spgist/spgutils.c:115-159
- This is a static function, only used within the spgutils.c file
- The function prioritizes opclass opcintype over actual input types to maintain compatibility
- Handles domain type flattening to provide base types to opclasses
- Includes optimization to avoid copying index expressions when they're already cached
- Contains error checking for mismatched expression counts
- The comment suggests this function might be moved elsewhere if other index access methods need similar functionality