# xmlconcat

## Location
[src/backend/utils/adt/xml.c:553-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L553-L618)

## Overview
Concatenates multiple XML values into a single XML document, handling XML declaration merging and validation.

## Definition
```c
xmltype *xmlconcat(List *args)
```

## Detailed Description
The xmlconcat function takes a list of XML values and concatenates them into a single XML document. It performs sophisticated handling of XML declarations by:

1. Parsing each input XML value to extract version and standalone attributes from XML declarations
2. Determining a global version and standalone value based on the inputs
3. Merging the content portions (excluding declarations) of all input XML values
4. Prepending a unified XML declaration if necessary

The function ensures that the resulting XML document has consistent declaration attributes. If all input documents have the same version, that version is used globally. If there are conflicts or missing versions, no version is included in the final declaration.

For standalone attributes:
- If any input has standalone="no", the result uses standalone="no"
- If any input has an unspecified standalone, the result has unspecified standalone
- Otherwise, standalone="yes" is used

## Parameters / Member Variables
- `args`: List of XML values to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - [xmltype](xmltype.md)
  - [DatumGetXmlP](../D/DatumGetXmlP.md)
  - VARSIZE
  - [text_to_cstring](../t/text_to_cstring.md)
  - [parse_xml_decl](../p/parse_xml_decl.md)
  - [print_xml_decl](../p/print_xml_decl.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - NO_XML_SUPPORT (fallback when libxml not available)
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (src/backend/executor/execExprInterp.c:3910)
  - [xmlconcat2](xmlconcat2.md) (src/backend/utils/adt/xml.c:631)
  - PG_RETURN_XML_P (src/include/utils/xml.h:72)

## Notes and Other Information
- Function is only available when PostgreSQL is compiled with libxml support (`USE_LIBXML`)
- TODO comment indicates that merging notations and unparsed entities is not implemented
- Handles complex XML declaration merging logic for proper XML document formation
- Memory management includes proper cleanup with pfree for temporary strings
- Used internally by PostgreSQL's XML expression evaluation system
- Returns NULL when libxml support is not available

## Simplified Source

```c
xmltype *xmlconcat(List *args)
{
#ifdef USE_LIBXML
    int global_standalone = 1;
    xmlChar *global_version = NULL;
    bool global_version_no_value = false;
    StringInfoData buf;

    initStringInfo(&buf);

    // Process each XML input
    foreach(ListCell *v, args)
    {
        xmltype *x = DatumGetXmlP(PointerGetDatum(lfirst(v)));
        size_t len;
        xmlChar *version;
        int standalone;
        char *str;

        // Extract XML content and parse declaration
        len = VARSIZE(x) - VARHDRSZ;
        str = text_to_cstring((text *) x);
        parse_xml_decl((xmlChar *) str, &len, &version, NULL, &standalone);

        // Merge standalone attributes (most restrictive wins)
        if (standalone == 0 && global_standalone == 1)
            global_standalone = 0;
        if (standalone < 0)
            global_standalone = -1;

        // Merge version info (must be consistent or omitted)
        if (!version)
            global_version_no_value = true;
        else if (!global_version)
            global_version = version;
        else if (xmlStrcmp(version, global_version) != 0)
            global_version_no_value = true;

        // Append content (without declaration)
        appendStringInfoString(&buf, str + len);
        pfree(str);
    }

    // Add unified XML declaration if needed
    if (!global_version_no_value || global_standalone >= 0)
    {
        StringInfoData buf2;
        initStringInfo(&buf2);

        print_xml_decl(&buf2,
                       (!global_version_no_value) ? global_version : NULL,
                       0, global_standalone);

        appendBinaryStringInfo(&buf2, buf.data, buf.len);
        buf = buf2;
    }

    return stringinfo_to_xmltype(&buf);
#else
    NO_XML_SUPPORT();
    return NULL;
#endif
}
```