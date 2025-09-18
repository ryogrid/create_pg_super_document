# XmlTableBuilderData

## Location
src/backend/utils/adt/xml.c: 196 - 208

## Overview
A builder structure that maintains state and context for constructing result sets from XML data using XPath expressions in PostgreSQL's XMLTABLE functionality.

## Definition


## Detailed Description
XmlTableBuilderData is the core data structure used by PostgreSQL's XMLTABLE functionality to extract tabular data from XML documents. It encapsulates all the necessary libxml contexts, compiled XPath expressions, and state information needed to efficiently process XML documents and generate result rows. The structure maintains both the XML parsing context and XPath evaluation context, along with compiled XPath expressions for performance optimization during row generation.

This structure is central to the XMLTABLE implementation, which allows SQL queries to extract structured data from XML documents using XPath expressions to define both row selection criteria and column extraction rules.

## Parameters / Member Variables
- : Magic number for structure validation and debugging purposes
- : Number of attributes (columns) in the target table structure
- : Running count of rows processed from the XML document
- : Pointer to PgXmlErrorContext for XML error handling and reporting
- : libxml parser context for XML document parsing operations
- : Parsed XML document structure maintained throughout processing
- : XPath evaluation context used for executing XPath expressions
- : Compiled XPath expression for row filtering/selection
- : Current XPath evaluation result object
- : Array of compiled XPath expressions for column value extraction

## Dependencies
- Functions called/Symbols referenced:
  - PgXmlErrorContext (for XML error handling)
  - xmlParserCtxtPtr (libxml parser context)
  - xmlDocPtr (libxml document structure)
  - xmlXPathContextPtr (libxml XPath context)
  - xmlXPathCompExprPtr (compiled XPath expressions)
  - xmlXPathObjectPtr (XPath evaluation results)
- Called from (representative examples):
  - XmlTableInitOpaque (initializes the builder structure)
  - XmlTableSetDocument (sets up XML document for processing)
  - XmlTableSetRowFilter (configures row selection XPath)
  - XmlTableSetColumnFilter (configures column extraction XPath)
  - XmlTableFetchRow (retrieves next row from XML)
  - XmlTableGetValue (extracts column values)
  - XmlTableDestroyOpaque (cleanup and memory deallocation)

## Notes and Other Information
- This structure is the private implementation detail of PostgreSQL's XMLTABLE functionality
- The magic field serves debugging and validation purposes during development
- XPath expressions are pre-compiled for performance optimization during table scanning
- The structure maintains libxml state across multiple row fetches for efficiency
- Memory management is crucial due to libxml resource allocation requirements
- Used exclusively within the XML table function implementation in xml.c
- The xpathscomp array size corresponds to natts for column-wise XPath evaluation
- Proper cleanup through XmlTableDestroyOpaque is essential to prevent memory leaks