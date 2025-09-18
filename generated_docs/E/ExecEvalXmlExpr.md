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
  - cstring_to_text_with_len
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
- String operations use StringInfo for efficient buffer management in XMLFOREST
- Error handling includes assertion checks for expected argument counts and types
- The function supports both named arguments (for attributes) and positional arguments (for content)
- NULL handling follows SQL standards where NULL inputs typically result in NULL outputs, with some exceptions for specific operations