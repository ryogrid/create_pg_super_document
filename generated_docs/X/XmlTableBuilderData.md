# XmlTableBuilderData

## Location
[src/backend/utils/adt/xml.c:196-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L196-L208)

## Overview
A builder structure that maintains state and context for constructing result sets from XML data using XPath expressions in PostgreSQL's XMLTABLE functionality.

## Definition

```c
typedef struct XmlTableBuilderData
{
	int			magic;
	int			natts;
	long int	row_count;
	PgXmlErrorContext *xmlerrcxt;
	xmlParserCtxtPtr ctxt;
	xmlDocPtr	doc;
	xmlXPathContextPtr xpathcxt;
	xmlXPathCompExprPtr xpathcomp;
	xmlXPathObjectPtr xpathobj;
	xmlXPathCompExprPtr *xpathscomp;
} XmlTableBuilderData;
```
## Detailed Description
XmlTableBuilderData is the core data structure used by PostgreSQL's XMLTABLE functionality to extract tabular data from XML documents. It encapsulates all the necessary libxml contexts, compiled XPath expressions, and state information needed to efficiently process XML documents and generate result rows. The structure maintains both the XML parsing context and XPath evaluation context, along with compiled XPath expressions for performance optimization during row generation.

This structure is central to the XMLTABLE implementation, which allows SQL queries to extract structured data from XML documents using XPath expressions to define both row selection criteria and column extraction rules.

## Parameters / Member Variables
- `magic`: Magic number for structure validation and debugging purposes
- `natts`: Number of attributes (columns) in the target table structure
- `row_count`: Running count of rows processed from the XML document
- `*xmlerrcxt`: Pointer to PgXmlErrorContext for XML error handling and reporting
- `ctxt`: libxml parser context for XML document parsing operations
- `doc`: Parsed XML document structure maintained throughout processing
- `xpathcxt`: XPath evaluation context used for executing XPath expressions
- `xpathcomp`: Compiled XPath expression for row filtering/selection
- `xpathobj`: Current XPath evaluation result object
- `*xpathscomp`: Array of compiled XPath expressions for column value extraction
## Dependencies
- Functions called/Symbols referenced:
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md) (for XML error handling)
  - xmlParserCtxtPtr (libxml parser context)
  - xmlDocPtr (libxml document structure)
  - xmlXPathContextPtr (libxml XPath context)
  - xmlXPathCompExprPtr (compiled XPath expressions)
  - xmlXPathObjectPtr (XPath evaluation results)
- Called from (representative examples):
  - [XmlTableInitOpaque](XmlTableInitOpaque.md) (initializes the builder structure)
  - [XmlTableSetDocument](XmlTableSetDocument.md) (sets up XML document for processing)
  - [XmlTableSetRowFilter](XmlTableSetRowFilter.md) (configures row selection XPath)
  - [XmlTableSetColumnFilter](XmlTableSetColumnFilter.md) (configures column extraction XPath)
  - [XmlTableFetchRow](XmlTableFetchRow.md) (retrieves next row from XML)
  - [XmlTableGetValue](XmlTableGetValue.md) (extracts column values)
  - [XmlTableDestroyOpaque](XmlTableDestroyOpaque.md) (cleanup and memory deallocation)

## Notes and Other Information
- This structure is the private implementation detail of PostgreSQL's XMLTABLE functionality
- The magic field serves debugging and validation purposes during development
- XPath expressions are pre-compiled for performance optimization during table scanning
- The structure maintains libxml state across multiple row fetches for efficiency
- Memory management is crucial due to libxml resource allocation requirements
- Used exclusively within the XML table function implementation in xml.c
- The xpathscomp array size corresponds to natts for column-wise XPath evaluation
- Proper cleanup through XmlTableDestroyOpaque is essential to prevent memory leaks