# tfuncInitialize

## Location
src/backend/executor/nodeTableFuncscan.c: 340 - 434

## Overview
This static function initializes a table function builder context by setting up the document, namespace declarations, row filters, and column filters for table functions like XMLTABLE or JSON_TABLE.

## Definition
```c
static void tfuncInitialize(TableFuncScanState *tstate, ExprContext *econtext, Datum doc)
```

## Detailed Description
tfuncInitialize prepares the table function execution environment by configuring all necessary components for processing structured data. It first installs the source document into the table function context, then evaluates and sets up namespace specifications for XML/JSON processing. The function also configures row filter expressions that determine which rows to extract from the source document. Finally, it sets up column filter expressions for each output column, using either explicit expressions or column names as defaults. The function includes comprehensive error checking to ensure all required expressions evaluate to non-null values.

## Parameters / Member Variables
- `tstate`: TableFuncScanState pointer containing the scan state and table function configuration
- `econtext`: ExprContext pointer providing the evaluation context for expressions
- `doc`: Datum containing the source document to be processed

## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalExpr
  - TextDatumGetCString
  - forboth
  - TupleDescAttr
  - NameStr
- Called from (representative examples):
  - [tfuncFetchRows](tfuncFetchRows.md)

## Notes and Other Information
- Handles namespace setup for XML and JSON processing contexts
- Implements comprehensive null-value checking with detailed error messages
- Supports ordinality columns by skipping filter setup for them
- Uses column names as default filters when explicit expressions are not provided
- Essential setup phase for XMLTABLE and JSON_TABLE functionality
- Properly integrates with PostgreSQL's expression evaluation system