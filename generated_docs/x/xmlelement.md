# xmlelement

## Location
[src/backend/utils/adt/xml.c:869-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L869-L992)

## Overview
Constructs an XML element with specified name, attributes, and content from PostgreSQL expressions, providing the core functionality for the SQL XMLELEMENT function.

## Definition
xmltype *xmlelement(XmlExpr *xexpr, Datum *named_argvalue, bool *named_argnull, Datum *argvalue, bool *argnull)

## Detailed Description
The xmlelement function creates XML elements by processing XmlExpr expressions that contain element names, attributes, and content. It takes pre-evaluated arguments to avoid potential conflicts with other libxml2 usage within the system.

The function processes two types of arguments: named arguments (which become XML attributes) and regular arguments (which become element content). Named arguments are processed first and added as XML attributes, while regular arguments are written as raw content within the element. NULL values in named arguments result in omitted attributes, while NULL content arguments are simply ignored.

The implementation uses libxml2 xmlTextWriter API to build well-formed XML output, ensuring proper escaping and formatting. It operates within a comprehensive error handling framework using PG_TRY/PG_CATCH blocks to manage libxml2 resources properly.

## Parameters / Member Variables
- `xexpr`: XmlExpr structure containing the element name, attribute names, and argument metadata
- `named_argvalue`: Array of Datum values for named arguments (attributes)
- `named_argnull`: Array of boolean flags indicating NULL status for named arguments
- `argvalue`: Array of Datum values for regular arguments (content)
- `argnull`: Array of boolean flags indicating NULL status for regular arguments

## Dependencies
- Functions called/Symbols referenced:
  - [map_sql_value_to_xml_value](../m/map_sql_value_to_xml_value.md) (SQL to XML value conversion)
  - [pg_xml_init](../p/pg_xml_init.md) (XML error context initialization)
  - [xml_ereport](xml_ereport.md) (XML-specific error reporting)
  - [xmlBuffer_to_xmltype](xmlBuffer_to_xmltype.md) (buffer to XML type conversion)
  - [pg_xml_done](../p/pg_xml_done.md) (XML context cleanup)
  - forboth (parallel list iteration macro)
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (expression evaluation in executor)
  - PG_RETURN_XML_P (via macro usage)

## Notes and Other Information
- Requires libxml2 support (USE_LIBXML compilation flag)
- Uses xmlTextWriter API for efficient XML construction
- Implements proper error handling with resource cleanup
- NULL named arguments become omitted attributes
- NULL content arguments are skipped entirely
- Arguments are pre-evaluated to avoid libxml2 conflicts
- Content is written as raw XML allowing nested elements
- Automatically handles XML character escaping for attributes
- Memory management includes proper cleanup in exception handlers
- Located in src/backend/utils/adt/xml.c at lines 869-992

## Simplified Source

```c
xmltype *xmlelement(XmlExpr *xexpr, Datum *named_argvalue, bool *named_argnull,
                    Datum *argvalue, bool *argnull)
{
#ifdef USE_LIBXML
    xmltype *result;
    List *named_arg_strings = NIL;
    List *arg_strings = NIL;
    PgXmlErrorContext *xmlerrcxt;
    volatile xmlBufferPtr buf = NULL;
    volatile xmlTextWriterPtr writer = NULL;

    // Convert named arguments (attributes) to strings
    int i = 0;
    foreach(ListCell *arg, xexpr->named_args)
    {
        Expr *e = (Expr *) lfirst(arg);
        char *str = named_argnull[i] ? NULL :
                    map_sql_value_to_xml_value(named_argvalue[i], exprType((Node *) e), false);
        named_arg_strings = lappend(named_arg_strings, str);
        i++;
    }

    // Convert regular arguments (content) to strings
    i = 0;
    foreach(ListCell *arg, xexpr->args)
    {
        if (!argnull[i])
        {
            Expr *e = (Expr *) lfirst(arg);
            char *str = map_sql_value_to_xml_value(argvalue[i], exprType((Node *) e), true);
            arg_strings = lappend(arg_strings, str);
        }
        i++;
    }

    // Initialize XML processing with error handling
    xmlerrcxt = pg_xml_init(PG_XML_STRICTNESS_ALL);

    PG_TRY();
    {
        // Create XML buffer and writer
        buf = xmlBufferCreate();
        if (buf == NULL || xmlerrcxt->err_occurred)
            xml_ereport(xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY, "could not allocate xmlBuffer");

        writer = xmlNewTextWriterMemory(buf, 0);
        if (writer == NULL || xmlerrcxt->err_occurred)
            xml_ereport(xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY, "could not allocate xmlTextWriter");

        // Write element start tag
        xmlTextWriterStartElement(writer, (xmlChar *) xexpr->name);

        // Write attributes from named arguments
        forboth(arg, named_arg_strings, narg, xexpr->arg_names)
        {
            char *str = (char *) lfirst(arg);
            char *argname = strVal(lfirst(narg));
            if (str)
                xmlTextWriterWriteAttribute(writer, (xmlChar *) argname, (xmlChar *) str);
        }

        // Write content from regular arguments
        foreach(ListCell *arg, arg_strings)
        {
            char *str = (char *) lfirst(arg);
            xmlTextWriterWriteRaw(writer, (xmlChar *) str);
        }

        // Close element and finalize
        xmlTextWriterEndElement(writer);
        xmlFreeTextWriter(writer);
        writer = NULL;

        result = xmlBuffer_to_xmltype(buf);
    }
    PG_CATCH();
    {
        // Cleanup on error
        if (writer) xmlFreeTextWriter(writer);
        if (buf) xmlBufferFree(buf);
        pg_xml_done(xmlerrcxt, true);
        PG_RE_THROW();
    }
    PG_END_TRY();

    xmlBufferFree(buf);
    pg_xml_done(xmlerrcxt, false);
    return result;
#else
    NO_XML_SUPPORT();
    return NULL;
#endif
}
```