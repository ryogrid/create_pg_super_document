# XmlExprOp

## Location
src/include/nodes/primnodes.h: 1588 - 1589

## Overview
XmlExprOp is an enumeration that defines the types of SQL/XML functions that require special grammar productions in PostgreSQL's XML functionality.

## Definition
```c
typedef enum XmlExprOp
{
    IS_XMLCONCAT,               /* XMLCONCAT(args) */
    IS_XMLELEMENT,              /* XMLELEMENT(name, xml_attributes, args) */
    IS_XMLFOREST,               /* XMLFOREST(xml_attributes) */
    IS_XMLPARSE,                /* XMLPARSE(text, is_doc, preserve_ws) */
    IS_XMLPI,                   /* XMLPI(name [, args]) */
    IS_XMLROOT,                 /* XMLROOT(xml, version, standalone) */
    IS_XMLSERIALIZE,            /* XMLSERIALIZE(is_document, xmlval, indent) */
    IS_DOCUMENT,                /* xmlval IS DOCUMENT */
} XmlExprOp;
```

## Detailed Description
XmlExprOp serves as an identifier for various SQL/XML functions that require special grammar productions in PostgreSQL. Each enumeration value corresponds to a specific XML-related SQL function with unique syntax and semantics. The enumeration is used within the XmlExpr structure to distinguish between different XML operations. The result type, typmod, and collation information are not stored directly but can be deduced from the specific XmlExprOp value, as the type and typmod fields in the associated structure are used primarily for display purposes.

## Parameters / Member Variables
- `IS_XMLCONCAT`: Represents the XMLCONCAT function for concatenating XML values
- `IS_XMLELEMENT`: Represents the XMLELEMENT function for creating XML elements with name, attributes, and content
- `IS_XMLFOREST`: Represents the XMLFOREST function for creating XML elements from a list of expressions
- `IS_XMLPARSE`: Represents the XMLPARSE function for parsing text into XML with document/content mode and whitespace preservation options
- `IS_XMLPI`: Represents the XMLPI function for creating XML processing instructions
- `IS_XMLROOT`: Represents the XMLROOT function for creating XML with version and standalone declarations
- `IS_XMLSERIALIZE`: Represents the XMLSERIALIZE function for serializing XML values to text with formatting options
- `IS_DOCUMENT`: Represents the IS DOCUMENT test for checking if an XML value is a well-formed document

## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enumeration)
- Called from (representative examples):
  - XmlExpr struct (used as op field type)

## Notes and Other Information
- Each operation corresponds to specific SQL/XML standard functionality
- The enumeration works in conjunction with XmlExpr structure fields like 'name', 'named_args', and 'arg_names'
- Result type information is deduced from the operation type rather than stored explicitly
- The 'name' field carries XML-escaped NAME arguments for relevant operations
- 'named_args' and 'arg_names' represent xml_attribute lists for operations that support attributes
- Type/typmod fields in the containing structure are used for display purposes only and may not represent the true result type