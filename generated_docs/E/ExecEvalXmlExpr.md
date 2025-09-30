# ExecEvalXmlExpr

## Location
[src/backend/executor/execExprInterp.c:3886-4100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3886-L4100)

## Overview
Evaluates various XML expression operations including concatenation, element construction, parsing, processing instructions, root manipulation, serialization, and document validation.

## Definition
void ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op)

## Detailed Description
This function handles the evaluation of all XML-related expressions in PostgreSQL through a comprehensive switch statement that processes different XML operation types. The function supports the full range of SQL/XML functionality including:

- **IS_XMLCONCAT**: Concatenates multiple XML values into a single XML document
- **IS_XMLFOREST**: Creates XML elements from named arguments, constructing a forest of XML elements
- **IS_XMLELEMENT**: Constructs a single XML element with attributes and content
- **IS_XMLPARSE**: Parses text input into XML format with optional whitespace preservation
- **IS_XMLPI**: Creates XML processing instructions with optional content
- **IS_XMLROOT**: Modifies the XML root element with version and standalone declarations
- **IS_XMLSERIALIZE**: Serializes XML to text format with formatting options
- **IS_DOCUMENT**: Validates whether an XML value represents a well-formed document

Each operation type has specific argument handling and uses specialized XML processing functions from PostgreSQL's XML subsystem. The function manages null value handling and proper memory management for XML data structures.

## Parameters / Member Variables
- : Expression state context (unused in this function)
- : Expression evaluation step containing XML expression data including operation type, argument values, argument null flags, and named argument information

## Dependencies
- Functions called/Symbols referenced:
  - [xmlconcat](../x/xmlconcat.md)
  - [xmlelement](../x/xmlelement.md)
  - [xmlparse](../x/xmlparse.md)
  - [xmlpi](../x/xmlpi.md)
  - [xmlroot](../x/xmlroot.md)
  - [xmltotext_with_options](../x/xmltotext_with_options.md)
  - [xml_is_document](../x/xml_is_document.md)
  - [map_sql_value_to_xml_value](../m/map_sql_value_to_xml_value.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - DatumGetTextPP
  - [DatumGetXmlP](../D/DatumGetXmlP.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [DatumGetBool](../D/DatumGetBool.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- Function initializes result to null and only sets non-null results when operations succeed
- XML operations require PostgreSQL to be built with XML support (--with-libxml configure option)
- Memory management is handled through PostgreSQL's memory context system
- [String](../S/String.md) operations use StringInfo for efficient buffer management in XMLFOREST
- Error handling includes assertion checks for expected argument counts and types
- The function supports both named arguments (for attributes) and positional arguments (for content)
- NULL handling follows SQL standards where NULL inputs typically result in NULL outputs, with some exceptions for specific operations

## Simplified Source

```c
void ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op)
{
    XmlExpr *xexpr = op->d.xmlexpr.xexpr;

    // Initialize result to null
    *op->resnull = true;
    *op->resvalue = (Datum) 0;

    switch (xexpr->op) {
        case IS_XMLCONCAT:
            {
                // Concatenate multiple XML values
                Datum *argvalue = op->d.xmlexpr.argvalue;
                bool *argnull = op->d.xmlexpr.argnull;
                List *values = NIL;

                // Collect non-null arguments
                for (int i = 0; i < list_length(xexpr->args); i++) {
                    if (!argnull[i])
                        values = lappend(values, DatumGetPointer(argvalue[i]));
                }

                if (values != NIL) {
                    *op->resvalue = PointerGetDatum(xmlconcat(values));
                    *op->resnull = false;
                }
            }
            break;

        case IS_XMLFOREST:
            {
                // Create XML forest from named arguments
                Datum *argvalue = op->d.xmlexpr.named_argvalue;
                bool *argnull = op->d.xmlexpr.named_argnull;
                StringInfoData buf;

                initStringInfo(&buf);

                // Build XML elements for each named argument
                int i = 0;
                ListCell *lc, *lc2;
                forboth(lc, xexpr->named_args, lc2, xexpr->arg_names) {
                    if (!argnull[i]) {
                        char *argname = strVal(lfirst(lc2));
                        Expr *e = (Expr *) lfirst(lc);

                        appendStringInfo(&buf, "<%s>%s</%s>",
                                       argname,
                                       map_sql_value_to_xml_value(argvalue[i], exprType((Node *) e), true),
                                       argname);
                        *op->resnull = false;
                    }
                    i++;
                }

                if (!*op->resnull) {
                    text *result = cstring_to_text_with_len(buf.data, buf.len);
                    *op->resvalue = PointerGetDatum(result);
                }
                pfree(buf.data);
            }
            break;

        case IS_XMLELEMENT:
            // Create XML element with attributes and content
            *op->resvalue = PointerGetDatum(xmlelement(xexpr,
                                                     op->d.xmlexpr.named_argvalue,
                                                     op->d.xmlexpr.named_argnull,
                                                     op->d.xmlexpr.argvalue,
                                                     op->d.xmlexpr.argnull));
            *op->resnull = false;
            break;

        case IS_XMLPARSE:
            {
                // Parse text into XML
                Datum *argvalue = op->d.xmlexpr.argvalue;
                bool *argnull = op->d.xmlexpr.argnull;

                if (argnull[0] || argnull[1])
                    return;

                text *data = DatumGetTextPP(argvalue[0]);
                bool preserve_whitespace = DatumGetBool(argvalue[1]);

                *op->resvalue = PointerGetDatum(xmlparse(data, xexpr->xmloption, preserve_whitespace));
                *op->resnull = false;
            }
            break;

        case IS_XMLPI:
            {
                // Create XML processing instruction
                text *arg = NULL;
                bool isnull = false;

                if (xexpr->args) {
                    isnull = op->d.xmlexpr.argnull[0];
                    if (!isnull)
                        arg = DatumGetTextPP(op->d.xmlexpr.argvalue[0]);
                }

                *op->resvalue = PointerGetDatum(xmlpi(xexpr->name, arg, isnull, op->resnull));
            }
            break;

        case IS_XMLROOT:
            {
                // Modify XML root element
                Datum *argvalue = op->d.xmlexpr.argvalue;
                bool *argnull = op->d.xmlexpr.argnull;

                if (argnull[0])
                    return;

                xmltype *data = DatumGetXmlP(argvalue[0]);
                text *version = argnull[1] ? NULL : DatumGetTextPP(argvalue[1]);
                int standalone = DatumGetInt32(argvalue[2]);

                *op->resvalue = PointerGetDatum(xmlroot(data, version, standalone));
                *op->resnull = false;
            }
            break;

        case IS_XMLSERIALIZE:
            {
                // Serialize XML to text
                if (op->d.xmlexpr.argnull[0])
                    return;

                Datum value = op->d.xmlexpr.argvalue[0];
                *op->resvalue = PointerGetDatum(xmltotext_with_options(DatumGetXmlP(value),
                                                                     xexpr->xmloption,
                                                                     xexpr->indent));
                *op->resnull = false;
            }
            break;

        case IS_DOCUMENT:
            {
                // Check if XML is a well-formed document
                if (op->d.xmlexpr.argnull[0])
                    return;

                Datum value = op->d.xmlexpr.argvalue[0];
                *op->resvalue = BoolGetDatum(xml_is_document(DatumGetXmlP(value)));
                *op->resnull = false;
            }
            break;

        default:
            elog(ERROR, "unrecognized XML operation");
            break;
    }
}
```