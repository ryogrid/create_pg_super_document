# XmlTableGetValue

## Location
[src/backend/utils/adt/xml.c:4927-5078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4927-L5078)

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
  - [GetXmlTableBuilderPrivateData](../G/GetXmlTableBuilderPrivateData.md)
  - xmlSetStructuredErrorFunc (libxml2)
  - [xml_errorHandler](../x/xml_errorHandler.md)
  - xmlXPathCompiledEval (libxml2)
  - [xml_ereport](../x/xml_ereport.md)
  - [xml_xmlnodetoxmltype](../x/xml_xmlnodetoxmltype.md)
  - [appendStringInfoText](../a/appendStringInfoText.md)
  - [xml_pstrdup_and_free](../x/xml_pstrdup_and_free.md)
  - [escape_xml](../e/escape_xml.md)
  - [get_type_category_preferred](../g/get_type_category_preferred.md)
  - [InputFunctionCall](../I/InputFunctionCall.md)
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

## Simplified Source

```c
static Datum
XmlTableGetValue(TableFuncScanState *state, int colnum,
                 Oid typid, int32 typmod, bool *isnull)
{
#ifdef USE_LIBXML
    XmlTableBuilderData *xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableGetValue");
    Datum result = (Datum) 0;
    xmlNodePtr cur;
    char *cstr = NULL;
    volatile xmlXPathObjectPtr xpathobj = NULL;

    Assert(xtCxt->xpathobj && xtCxt->xpathobj->type == XPATH_NODESET &&
           xtCxt->xpathobj->nodesetval != NULL);

    xmlSetStructuredErrorFunc((void *) xtCxt->xmlerrcxt, xml_errorHandler);
    *isnull = false;

    // Get current row node and set as XPath context
    cur = xtCxt->xpathobj->nodesetval->nodeTab[xtCxt->row_count - 1];
    Assert(xtCxt->xpathscomp[colnum] != NULL);

    PG_TRY();
    {
        xtCxt->xpathcxt->node = cur;

        // Evaluate column XPath expression
        xpathobj = xmlXPathCompiledEval(xtCxt->xpathscomp[colnum], xtCxt->xpathcxt);
        if (xpathobj == NULL || xtCxt->xmlerrcxt->err_occurred)
            xml_ereport(xtCxt->xmlerrcxt, ERROR, ERRCODE_INTERNAL_ERROR,
                        "could not create XPath object");

        // Handle different XPath result types
        if (xpathobj->type == XPATH_NODESET)
        {
            int count = (xpathobj->nodesetval != NULL) ? xpathobj->nodesetval->nodeNr : 0;

            if (count == 0)
            {
                *isnull = true;
            }
            else if (typid == XMLOID)
            {
                // For XML columns: concatenate all nodes
                StringInfoData str;
                initStringInfo(&str);
                for (int i = 0; i < count; i++)
                {
                    text *textstr = xml_xmlnodetoxmltype(xpathobj->nodesetval->nodeTab[i],
                                                         xtCxt->xmlerrcxt);
                    appendStringInfoText(&str, textstr);
                }
                cstr = str.data;
            }
            else
            {
                // For non-XML columns: require single node
                if (count > 1)
                    ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                            errmsg("more than one value returned by column XPath expression")));

                xmlChar *str = xmlXPathCastNodeSetToString(xpathobj->nodesetval);
                cstr = str ? xml_pstrdup_and_free(str) : "";
            }
        }
        else if (xpathobj->type == XPATH_STRING)
        {
            cstr = (typid == XMLOID) ? escape_xml((char *) xpathobj->stringval)
                                     : (char *) xpathobj->stringval;
        }
        else if (xpathobj->type == XPATH_BOOLEAN)
        {
            // Handle boolean results with type conversion
            char typcategory;
            bool typispreferred;
            get_type_category_preferred(typid, &typcategory, &typispreferred);

            xmlChar *str = (typcategory != TYPCATEGORY_NUMERIC)
                ? xmlXPathCastBooleanToString(xpathobj->boolval)
                : xmlXPathCastNumberToString(xmlXPathCastBooleanToNumber(xpathobj->boolval));
            cstr = xml_pstrdup_and_free(str);
        }
        else if (xpathobj->type == XPATH_NUMBER)
        {
            xmlChar *str = xmlXPathCastNumberToString(xpathobj->floatval);
            cstr = xml_pstrdup_and_free(str);
        }
        else
            elog(ERROR, "unexpected XPath object type %u", xpathobj->type);

        // Convert to target PostgreSQL type
        if (!*isnull)
            result = InputFunctionCall(&state->in_functions[colnum], cstr,
                                       state->typioparams[colnum], typmod);
    }
    PG_FINALLY();
    {
        if (xpathobj != NULL)
            xmlXPathFreeObject(xpathobj);
    }
    PG_END_TRY();

    return result;
#else
    NO_XML_SUPPORT();
    return 0;
#endif
}
```