# XmlTableFetchRow

## Location
[src/backend/utils/adt/xml.c:4882-4926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4882-L4926)

## Overview
Prepares the next "current" tuple for upcoming GetValue calls by evaluating the row-filter XPath expression and advancing to the next matching row in the XML document.

## Definition
```c
static bool XmlTableFetchRow(TableFuncScanState *state)
```

## Detailed Description
XmlTableFetchRow is a static function that implements the row iteration logic for XML table processing. It evaluates the previously compiled row-filter XPath expression to find matching nodes in the XML document and advances the current row position. The function performs lazy evaluation - it only executes the XPath expression on the first call, then iterates through the resulting node set on subsequent calls.

The function returns true when a new row is available for processing, and false when no more rows match the filter criteria. It handles XPath evaluation errors and ensures proper error context propagation to libxml2. The function maintains a row counter to track the current position within the result set.

## Parameters / Member Variables
- `state`: TableFuncScanState pointer containing the scanning state and private data for XML table processing

## Return Value
- Returns `true` if a new row is available for processing
- Returns `false` if no more rows are available (end of result set)

## Dependencies
- Functions called/Symbols referenced:
  - [GetXmlTableBuilderPrivateData](../G/GetXmlTableBuilderPrivateData.md)
  - xmlSetStructuredErrorFunc (libxml2)
  - [xml_errorHandler](../x/xml_errorHandler.md)
  - xmlXPathCompiledEval (libxml2)
  - [xml_ereport](../x/xml_ereport.md)
  - NO_XML_SUPPORT
- Called from (representative examples):
  - No direct callers found in codebase analysis

## Notes and Other Information
- This function is only available when compiled with USE_LIBXML support
- When libxml2 is not available, the function calls NO_XML_SUPPORT() and returns false
- The function performs lazy evaluation of the XPath expression - evaluation occurs only on the first call
- Uses xmlSetStructuredErrorFunc to ensure proper error context propagation to libxml2
- Only processes XPATH_NODESET type results; other XPath result types return false immediately
- Maintains an internal row_count to track iteration through the node set
- The function assumes that XmlTableSetRowFilter and XmlTableSetDocument have been called previously
- Returns false for empty node sets or when all nodes have been processed