# TableFunc

## Location
src/include/nodes/primnodes.h: 109 - 146

## Overview
TableFunc represents table functions such as XMLTABLE and JSON_TABLE, providing a structured way to extract tabular data from structured documents with column definitions and filtering capabilities.

## Definition


## Detailed Description
TableFunc is a comprehensive node structure designed to represent table functions that transform structured documents (XML or JSON) into relational tabular form. It serves as the foundation for XMLTABLE and JSON_TABLE functionality in PostgreSQL, providing a rich set of capabilities for document processing.

The structure supports namespace handling for XML documents, column type specifications with full metadata (types, type modifiers, collations), filtering expressions at both row and column levels, and default value handling. For JSON_TABLE specifically, it includes additional features like value expressions, passing arguments, and execution plans.

Many fields are marked with pg_node_attr(query_jumble_ignore) to exclude them from query fingerprinting, as they represent metadata rather than core query logic.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL's node system type identification
- : TableFuncType enum indicating XMLTABLE or JSON_TABLE
- : List of namespace URI expressions for XML namespace handling
- : List of namespace names (String nodes or NULL for DEFAULT)
- : Expression providing the input document to process
- : Expression for filtering/selecting rows from the document
- : List of String nodes containing output column names
- : List of OIDs representing the data types of output columns
- : List of integers specifying type modifiers for columns
- : List of OIDs specifying collation for each column
- : List of expressions for extracting/computing column values
- : List of default value expressions for columns
- : JSON_TABLE specific: column value extraction expressions
- : JSON_TABLE specific: PASSING clause argument expressions
- : Bitmapset indicating which columns are NOT NULL
- : JSON_TABLE specific: execution plan node
- : Index of ordinality column (0-based, -1 if none)
- : Parse location in original query for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node system)
  - TableFuncType (function type enumeration)
  - List (PostgreSQL list structure)
  - Node (generic node structure)
  - Bitmapset (bitmap utilities)
  - ParseLoc (parse location tracking)

- Called from (representative examples):
  - transformRangeTableFunc (parsing table functions)
  - transformJsonTable (JSON_TABLE parsing)
  - addRangeTableEntryForTableFunc (RTE creation)
  - ExecInitTableFuncScan (execution initialization)
  - create_tablefuncscan_plan (plan creation)
  - get_tablefunc (rule decompilation)

## Notes and Other Information
- Central to PostgreSQL's structured document processing capabilities
- Supports both XMLTABLE and JSON_TABLE SQL standard functions
- Many metadata fields excluded from query jumbling for performance
- Handles complex column specifications with full type system integration
- JSON_TABLE includes additional execution planning and argument passing
- Ordinality columns provide row numbering functionality
- Extensive namespace support for XML document processing
- Critical component of the Range Table Entry system for table functions