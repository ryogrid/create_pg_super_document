# RangeTableFuncCol

## Location
[src/include/nodes/parsenodes.h:673-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L673-L683)

## Overview
RangeTableFuncCol represents a single column definition within a RangeTableFunc, specifying the column's name, data type, extraction expression, and various column properties for table functions like XMLTABLE.

## Definition
```c
typedef struct RangeTableFuncCol
{
    NodeTag     type;
    char       *colname;        /* name of generated column */
    TypeName   *typeName;       /* type of generated column */
    bool        for_ordinality; /* does it have FOR ORDINALITY? */
    bool        is_not_null;    /* does it have NOT NULL? */
    Node       *colexpr;        /* column filter expression */
    Node       *coldefexpr;     /* column default value expression */
    ParseLoc    location;       /* token location, or -1 if unknown */
} RangeTableFuncCol;
```

## Detailed Description
RangeTableFuncCol defines a single column specification within a table function's column list. Each column can have a name, data type, extraction expression, and default value. The special FOR ORDINALITY clause creates an int4 column that provides row numbering, and when this flag is set, most other fields are ignored. The structure captures both the column metadata and the expressions needed to extract or generate column values.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a RangeTableFuncCol node
- `colname`: Name of the generated column in the result set
- `typeName`: Data type specification for the column (TypeName structure)
- `for_ordinality`: Boolean flag indicating this is a FOR ORDINALITY column (row number)
- `is_not_null`: Boolean flag indicating whether the column has a NOT NULL constraint
- `colexpr`: Expression used to extract/filter the column value from source data
- `coldefexpr`: Default value expression for the column when no data is found
- `location`: Parse location for error reporting, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - [TypeName](../T/TypeName.md)
  - ParseLoc
- Called from (representative examples):
  - [transformRangeTableFunc](../t/transformRangeTableFunc.md)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:673-683
- When for_ordinality is true, the column becomes an int4 auto-numbering column and other fields are largely ignored
- Part of the RangeTableFunc structure's columns list
- Supports both value extraction expressions and default value expressions for robust data handling