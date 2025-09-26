# XmlTableSetRowFilter

## Location
[src/backend/utils/adt/xml.c:4815-4846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4815-L4846)

## Overview
Installs the row-filter XPath expression for XML table processing, compiling and validating the provided XPath expression to filter rows during XML table scanning.

## Definition

```c
static void
XmlTableSetRowFilter(TableFuncScanState *state, const char *path)
```
## Detailed Description
XmlTableSetRowFilter is a static function that sets up row filtering for XML table operations by compiling an XPath expression. The function validates that the provided path is not empty, converts it to libxml2's xmlChar format, and compiles it into an XPath expression using the existing XPath context. This compiled expression will later be used to filter which rows from the XML document should be included in the table scan results.

The function requires that XmlTableSetDocument has been called previously to establish the XPath context. It performs error checking for empty paths and invalid XPath syntax, reporting appropriate PostgreSQL errors when issues are encountered.

## Parameters / Member Variables
- : TableFuncScanState pointer containing the scanning state and private data for XML table processing
- : Null-terminated string containing the XPath expression to be used for row filtering

## Dependencies
- Functions called/Symbols referenced:
  - [GetXmlTableBuilderPrivateData](../G/GetXmlTableBuilderPrivateData.md)
  - [pg_xmlCharStrndup](../p/pg_xmlCharStrndup.md)
  - xmlXPathCtxtCompile (libxml2)
  - [xml_ereport](../x/xml_ereport.md)
  - NO_XML_SUPPORT
- Called from (representative examples):
  - No direct callers found in codebase analysis

## Notes and Other Information
- This function is only available when compiled with USE_LIBXML support
- When libxml2 is not available, the function calls NO_XML_SUPPORT() which raises an error
- The function assumes that XmlTableSetDocument has been called previously to set up the XPath context
- Empty path strings are explicitly rejected with a DATA_EXCEPTION error
- Invalid XPath expressions result in SYNTAX_ERROR exceptions
- The compiled XPath expression is stored in the XmlTableBuilderData structure for later use during row filtering