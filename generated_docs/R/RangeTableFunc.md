# RangeTableFunc

## Location
src/include/nodes/parsenodes.h: 655 - 665

## Overview
RangeTableFunc represents the raw parsed form of "table functions" such as XMLTABLE in PostgreSQL's parse tree, containing the necessary information to transform document expressions into table-like structures with defined columns.

## Definition


## Detailed Description
RangeTableFunc is a parse node structure that represents table functions like XMLTABLE during the parsing phase. These functions take a document (typically XML) and extract tabular data from it based on specified expressions and column definitions. The structure captures all the syntactic elements needed to later transform this into an executable plan. Note that JSON_TABLE uses a separate JsonTable node rather than RangeTableFunc.

## Parameters / Member Variables
- : NodeTag identifying this as a RangeTableFunc node
- : Boolean flag indicating whether the LATERAL keyword was specified
- : Pointer to the document expression that provides the source data
- : Pointer to the row generator expression that defines how to extract rows
- : List of namespace definitions as ResTarget nodes
- : List of RangeTableFuncCol structures defining the output columns
- : Table alias and optional column aliases for the result
- : Parse location for error reporting, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - Alias
  - ParseLoc
- Called from (representative examples):
  - transformRangeTableFunc
  - transformFromClauseItem
  - raw_expression_tree_walker_impl

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:655-665
- This structure is specific to XMLTABLE and similar table functions
- JSON_TABLE uses a different node type (JsonTable) for its implementation
- The LATERAL keyword affects how the table function can reference columns from preceding tables in the FROM clause