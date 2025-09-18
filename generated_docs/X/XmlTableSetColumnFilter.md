# XmlTableSetColumnFilter

## Location
src/backend/utils/adt/xml.c: 4847 - 4881

## Overview
Installs the column-filter XPath expression for a specific column in XML table processing, compiling and storing the XPath expression for later use in extracting column values.

## Definition
```c
static void XmlTableSetColumnFilter(TableFuncScanState *state, const char *path, int colnum)
```

## Detailed Description
XmlTableSetColumnFilter is a static function that sets up column-specific filtering for XML table operations. It compiles an XPath expression that will be used to extract values for a particular column from XML documents. The function validates the provided path, converts it to libxml2's xmlChar format, and compiles it into an XPath expression using the existing XPath context. The compiled expression is stored in an array indexed by column number for efficient retrieval during value extraction.

Similar to XmlTableSetRowFilter, this function requires that XmlTableSetDocument has been called previously to establish the XPath context. It performs validation for valid pointers, empty paths, and invalid XPath syntax.

## Parameters / Member Variables
- `state`: TableFuncScanState pointer containing the scanning state and private data for XML table processing
- `path`: Null-terminated string containing the XPath expression to be used for extracting values from this column
- `colnum`: Integer index of the column for which this XPath expression applies

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - [GetXmlTableBuilderPrivateData](../G/GetXmlTableBuilderPrivateData.md)
  - pg_xmlCharStrndup
  - xmlXPathCtxtCompile (libxml2)
  - xml_ereport
  - NO_XML_SUPPORT
- Called from (representative examples):
  - No direct callers found in codebase analysis

## Notes and Other Information
- This function is only available when compiled with USE_LIBXML support
- When libxml2 is not available, the function calls NO_XML_SUPPORT() which raises an error
- The function assumes that XmlTableSetDocument has been called previously to set up the XPath context
- Empty path strings are explicitly rejected with a DATA_EXCEPTION error
- Invalid XPath expressions result in DATA_EXCEPTION errors (different from row filter which uses SYNTAX_ERROR)
- The compiled XPath expression is stored in the xpathscomp array at the specified column index
- Uses PointerIsValid assertion to ensure the path parameter is not NULL before processing