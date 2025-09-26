# XmlTableDestroyOpaque

## Location
[src/backend/utils/adt/xml.c:5079-5118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L5079-L5118)

## Overview
Releases all libxml2 resources and cleans up the XML table processing context, ensuring proper resource deallocation and preventing memory leaks.

## Definition
```c
static void XmlTableDestroyOpaque(TableFuncScanState *state)
```

## Detailed Description
XmlTableDestroyOpaque is a cleanup function that systematically releases all libxml2 resources allocated during XML table processing. It handles the deallocation of compiled XPath expressions (both row and column filters), XPath result objects, XPath context, XML document, and parser context. The function follows a defensive programming approach by checking for NULL pointers before attempting to free resources.

The function also performs final cleanup of the PostgreSQL XML error context and invalidates the magic number to prevent further use of the destroyed context. It sets the opaque pointer to NULL to indicate that the context is no longer valid.

## Parameters / Member Variables
- `state`: TableFuncScanState pointer containing the scanning state and private data for XML table processing

## Dependencies
- Functions called/Symbols referenced:
  - [GetXmlTableBuilderPrivateData](../G/GetXmlTableBuilderPrivateData.md)
  - xmlSetStructuredErrorFunc (libxml2)
  - [xml_errorHandler](../x/xml_errorHandler.md)
  - xmlXPathFreeCompExpr (libxml2)
  - xmlXPathFreeObject (libxml2)
  - xmlXPathFreeContext (libxml2)
  - xmlFreeDoc (libxml2)
  - xmlFreeParserCtxt (libxml2)
  - [pg_xml_done](../p/pg_xml_done.md)
  - NO_XML_SUPPORT
- Called from (representative examples):
  - No direct callers found in codebase analysis

## Notes and Other Information
- This function is only available when compiled with USE_LIBXML support
- When libxml2 is not available, the function calls NO_XML_SUPPORT() which raises an error
- The function performs defensive NULL checks before freeing each libxml2 resource to prevent segmentation faults
- Iterates through all column XPath expressions (xpathscomp array) to free each compiled expression
- Frees resources in a logical order: expressions, objects, contexts, documents, and parser contexts
- Calls pg_xml_done with cleanup flag set to true to finalize PostgreSQL XML error context
- Invalidates the magic number in the context structure to prevent use-after-free scenarios
- Sets state->opaque to NULL to indicate the context is destroyed and should not be used
- Should be called as part of the cleanup phase of XML table processing to prevent memory leaks
- Ensures proper error context propagation to libxml2 even during cleanup operations