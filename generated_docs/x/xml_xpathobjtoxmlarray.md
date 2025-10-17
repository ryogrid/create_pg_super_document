# xml_xpathobjtoxmlarray

## Location
[src/backend/utils/adt/xml.c:4246-4323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4246-L4323)

## Overview
Converts an XML XPath evaluation result object into a PostgreSQL array of XML values, handling different XPath result types (nodesets, primitives) appropriately.

## Definition
```c
static int xml_xpathobjtoxmlarray(xmlXPathObjectPtr xpathobj,
                                 ArrayBuildState *astate,
                                 PgXmlErrorContext *xmlerrcxt)
```

## Detailed Description
This function processes the result of XPath expression evaluation and converts it to a PostgreSQL array format. It handles four main XPath result types: XPATH_NODESET (converted to array of XML text representations), XPATH_BOOLEAN, XPATH_NUMBER, and XPATH_STRING (each converted to single-element arrays). For nodesets, each node is converted using xml_xmlnodetoxmltype and added to the array. For scalar values, they are converted to their string representations and then to xmltype. The function can operate in counting mode (when astate is NULL) to just return the number of elements without building the array.

## Parameters / Member Variables
- `xpathobj`: Pointer to libxml2 XPath result object containing the evaluation result
- `astate`: PostgreSQL ArrayBuildState for constructing the result array (NULL for counting mode)
- `xmlerrcxt`: PostgreSQL XML error context for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [xml_xmlnodetoxmltype](xml_xmlnodetoxmltype.md)
  - [accumArrayResult](../a/accumArrayResult.md)
  - [map_sql_value_to_xml_value](../m/map_sql_value_to_xml_value.md)
  - [cstring_to_xmltype](../c/cstring_to_xmltype.md)
  - [BoolGetDatum](../B/BoolGetDatum.md), Float8GetDatum, CStringGetDatum (PostgreSQL datum conversion)
- Called from (representative examples):
  - [xpath_internal](xpath_internal.md)

## Notes and Other Information
- Static function used internally by PostgreSQL's XPath implementation
- Supports both counting mode (returns element count without building array) and building mode
- Handles all standard XPath result types defined by the XPath specification
- Converts all results to PostgreSQL's xmltype for consistency
- Uses PostgreSQL's array building infrastructure (ArrayBuildState/accumArrayResult)
- Part of the bridge between libxml2's XPath engine and PostgreSQL's type system
- Error handling relies on the xml_xmlnodetoxmltype function for node conversion errors

## Simplified Source

```c
static int xml_xpathobjtoxmlarray(xmlXPathObjectPtr xpathobj,
                                 ArrayBuildState *astate,
                                 PgXmlErrorContext *xmlerrcxt)
{
    int result = 0;
    Datum datum;
    Oid datumtype;
    char *result_str;

    switch (xpathobj->type) {
        case XPATH_NODESET:
            // Handle nodeset: convert each node to XML and add to array
            if (xpathobj->nodesetval != NULL) {
                result = xpathobj->nodesetval->nodeNr;
                if (astate != NULL) {
                    for (int i = 0; i < result; i++) {
                        datum = PointerGetDatum(xml_xmlnodetoxmltype(xpathobj->nodesetval->nodeTab[i], xmlerrcxt));
                        accumArrayResult(astate, datum, false, XMLOID, CurrentMemoryContext);
                    }
                }
            }
            return result;

        case XPATH_BOOLEAN:
            // Handle boolean: return 1 element count or convert to datum
            if (astate == NULL) return 1;
            datum = BoolGetDatum(xpathobj->boolval);
            datumtype = BOOLOID;
            break;

        case XPATH_NUMBER:
            // Handle number: return 1 element count or convert to datum
            if (astate == NULL) return 1;
            datum = Float8GetDatum(xpathobj->floatval);
            datumtype = FLOAT8OID;
            break;

        case XPATH_STRING:
            // Handle string: return 1 element count or convert to datum
            if (astate == NULL) return 1;
            datum = CStringGetDatum((char *) xpathobj->stringval);
            datumtype = CSTRINGOID;
            break;

        default:
            elog(ERROR, "xpath expression result type %d is unsupported", xpathobj->type);
            return 0;
    }

    // Convert scalar values to XML and add to array
    result_str = map_sql_value_to_xml_value(datum, datumtype, true);
    datum = PointerGetDatum(cstring_to_xmltype(result_str));
    accumArrayResult(astate, datum, false, XMLOID, CurrentMemoryContext);
    return 1;
}
```