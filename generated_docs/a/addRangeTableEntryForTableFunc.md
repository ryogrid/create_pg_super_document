# addRangeTableEntryForTableFunc

## Location
src/backend/parser/parse_relation.c: 2049 - 2133

## Overview
Creates a range table entry for table functions (XMLTABLE and JSON_TABLE) and adds it to the parser state, returning a ParseNamespaceItem with predefined column structure and type information.

## Definition


## Detailed Description
The  function creates range table entries specifically for PostgreSQL's table functions like XMLTABLE and JSON_TABLE. These functions transform structured data (XML or JSON) into relational table format with predefined columns and types.

Key characteristics:
1. **Table Function Support**: Handles XMLTABLE and JSON_TABLE constructs that parse structured data
2. **Predefined Schema**: Uses column definitions from the TableFunc node rather than dynamic type resolution
3. **Column Validation**: Ensures column count limits and validates alias specifications
4. **Type Information Copying**: Directly copies column types, type modifiers, and collations from the TableFunc
5. **Auto-naming**: Provides default names ("xmltable" or "json_table") when no alias is specified

The function performs validation to ensure:
- Column count doesn't exceed MaxTupleAttributeNumber
- All column metadata lists have consistent lengths
- Provided aliases don't exceed the number of defined columns
- Missing alias columns are filled in from the TableFunc's column names

Unlike other range table entry functions, this one works with a fully-specified TableFunc node that already contains all necessary column metadata, making the process more straightforward than dynamic type resolution.

## Parameters / Member Variables
- : Parser state containing the range table and other parsing context
- : TableFunc node containing column definitions, types, and function-specific information
- : Optional alias for the table function; if NULL, uses "xmltable" or "json_table"
- : Boolean indicating whether this is a LATERAL table function reference
- : Boolean indicating whether this entry originates from a FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RTE creation)
  - makeAlias (for alias creation)
  - copyObject (for alias copying)
  - pstrdup (string duplication)
  - list_concat, list_copy_tail (list manipulation for column names)
  - lappend (list manipulation)
  - buildNSItemFromLists (namespace item creation)
  - ereport (error reporting)
  - exprLocation (error position reporting)
- Called from (representative examples):
  - transformRangeTableFunc (in parse_clause.c)
  - transformJsonTable (in parse_jsontable.c)

## Notes and Other Information
- Table functions are never checked for access rights since they represent computed results from structured data parsing
- The TableFunc node must have consistent column metadata - all column-related lists must have the same length
- Supports both XMLTABLE (TFT_XMLTABLE) and JSON_TABLE function types
- Column aliases can be partially specified - missing aliases are automatically filled from the TableFunc's column names
- Error messages are context-specific, distinguishing between XMLTABLE and JSON_TABLE in user-facing messages
- The function assumes the TableFunc node has been properly validated and constructed by earlier parsing stages
- LATERAL table functions have special scoping rules allowing them to reference columns from preceding FROM items
- Column count validation prevents exceeding PostgreSQL's tuple attribute limits (MaxTupleAttributeNumber)