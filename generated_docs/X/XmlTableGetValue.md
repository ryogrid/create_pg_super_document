# XmlTableGetValue

## Location
src/backend/utils/adt/xml.c: 4927 - 5078

## Overview
Returns the value for a specified column number for the current row in XML table processing, extracting and converting the value using the column's XPath expression to the target PostgreSQL data type.

## Definition
```c
static Datum XmlTableGetValue(TableFuncScanState *state, int colnum, Oid typid, int32 typmod, bool *isnull)
```

## Detailed Description
XmlTableGetValue is a comprehensive function that extracts column values from XML documents during table scanning operations. It uses the current row context established by XmlTableFetchRow and the column-specific XPath expression set by XmlTableSetColumnFilter to evaluate and extract values.

The function handles multiple XPath result types (XPATH_NODESET, XPATH_STRING, XPATH_BOOLEAN, XPATH_NUMBER) and performs appropriate type conversions based on the target PostgreSQL column type. For XML columns, it can concatenate multiple nodes; for non-XML columns, it enforces single-value constraints and performs type coercion as needed.

The function includes comprehensive error handling using PostgreSQL's PG_TRY/PG_FINALLY mechanism to ensure proper cleanup of libxml2 resources. It also handles special cases like null values, boolean-to-numeric conversions, and XML escaping.

## Parameters / Member Variables
- `state`: TableFuncScanState pointer containing the scanning state and private data for XML table processing
- `colnum`: Integer index of the column for which to extract the value
- `typid`: OID of the target PostgreSQL data type for the column
- `typmod`: Type modifier for the target data type
- `isnull`: Pointer to boolean flag that will be set to true if the result is NULL

## Return Value
- Returns a `Datum` containing the extracted and converted column value
- Sets `*isnull` to true if no value is found or the result should be NULL

## Dependencies
- Functions called/Symbols referenced:
  - GetXmlTableBuilderPrivateData
  - xmlSetStructuredErrorFunc (libxml2)
  - xml_errorHandler
  - xmlXPathCompiledEval (libxml2)
  - xml_ereport
  - xml_xmlnodetoxmltype
  - appendStringInfoText
  - xml_pstrdup_and_free
  - escape_xml
  - get_type_category_preferred
  - InputFunctionCall
  - xmlXPathFreeObject (libxml2)
  - NO_XML_SUPPORT
- Called from (representative examples):
  - No direct callers found in codebase analysis

## Notes and Other Information
- This function is only available when compiled with USE_LIBXML support
- The function leaks memory and should be called in a context that is reset frequently
- Handles four main XPath result scenarios: no nodes (NULL), XML target type (concatenate all), single node (return content), multiple nodes (error for non-XML types)
- For XPATH_NODESET results with multiple nodes and non-XML target types, raises CARDINALITY_VIOLATION error
- Supports implicit casting from XPath boolean results to numeric PostgreSQL types
- Uses PostgreSQL's input functions to convert string representations to target data types
- Implements proper resource cleanup using PG_TRY/PG_FINALLY to ensure XPath objects are freed
- Sets the current XML node as the context for column XPath evaluation
- For XML target columns, properly escapes string values and concatenates multiple node results