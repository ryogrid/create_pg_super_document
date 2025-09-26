# transformXmlExpr

## Location
src/backend/parser/parse_expr.c: 2355 - 2483

## Overview
Transforms XML expression nodes during parsing by converting raw XML expressions into properly typed and validated expressions with appropriate type coercion for different XML operations.

## Definition


## Detailed Description
The  function is responsible for transforming XML expressions during the parsing phase. It creates a new XmlExpr node from the input, handling named arguments, argument names, and applying appropriate type coercion based on the specific XML operation type (XMLCONCAT, XMLELEMENT, XMLFOREST, XMLPARSE, XMLPI, XMLROOT, IS_DOCUMENT).

The function processes named arguments by transforming ResTarget nodes, extracting argument names, and validating them. For XMLELEMENT operations, it checks for duplicate attribute names. It then processes regular arguments with operation-specific type coercion - for example, XMLCONCAT and XMLFOREST arguments are coerced to XML type, while XMLPARSE arguments require TEXT for the first argument and BOOLEAN for subsequent ones.

## Parameters / Member Variables
- : ParseState context for the current parsing operation
- : Input XmlExpr node to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - map_sql_identifier_to_xml_name
  - transformExprRecurse
  - FigureColname
  - coerce_to_specific_type
  - coerce_to_boolean
  - makeString
  - lappend
- Called from (representative examples):
  - transformExprRecurse

## Notes and Other Information
- The function sets the output type to XMLOID to mark the node as transformed
- XMLSERIALIZE operations are not handled in this function (assertion failure occurs)
- Named argument processing includes validation for duplicate attribute names in XMLELEMENT
- Different XML operations require different type coercions for their arguments
- The function is located in src/backend/parser/parse_expr.c:2355-2483