# XmlExpr

## Location
[src/include/nodes/primnodes.h:1596-1618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1596-L1618)

## Overview
XmlExpr represents various SQL/XML functions that require special grammar productions, providing a unified structure for XML-related operations like XMLCONCAT, XMLELEMENT, XMLPARSE, etc.

## Definition
```c
typedef enum XmlExprOp
{
    IS_XMLCONCAT,       /* XMLCONCAT(args) */
    IS_XMLELEMENT,      /* XMLELEMENT(name, xml_attributes, args) */
    IS_XMLFOREST,       /* XMLFOREST(xml_attributes) */
    IS_XMLPARSE,        /* XMLPARSE(text, is_doc, preserve_ws) */
    IS_XMLPI,           /* XMLPI(name [, args]) */
    IS_XMLROOT,         /* XMLROOT(xml, version, standalone) */
    IS_XMLSERIALIZE,    /* XMLSERIALIZE(is_document, xmlval, indent) */
    IS_DOCUMENT,        /* xmlval IS DOCUMENT */
} XmlExprOp;

typedef enum XmlOptionType
{
    XMLOPTION_DOCUMENT,
    XMLOPTION_CONTENT,
} XmlOptionType;

typedef struct XmlExpr
{
    Expr        xpr;
    /* xml function ID */
    XmlExprOp   op;
    /* name in xml(NAME foo ...) syntaxes */
    char       *name pg_node_attr(query_jumble_ignore);
    /* non-XML expressions for xml_attributes */
    List       *named_args;
    /* parallel list of String values */
    List       *arg_names pg_node_attr(query_jumble_ignore);
    /* list of expressions */
    List       *args;
    /* DOCUMENT or CONTENT */
    XmlOptionType xmloption pg_node_attr(query_jumble_ignore);
    /* INDENT option for XMLSERIALIZE */
    bool        indent;
    /* target type/typmod for XMLSERIALIZE */
    Oid         type pg_node_attr(query_jumble_ignore);
    int32       typmod pg_node_attr(query_jumble_ignore);
    /* token location, or -1 if unknown */
    ParseLoc    location;
} XmlExpr;
```

## Detailed Description
XmlExpr is a node structure that represents various SQL/XML functions requiring special grammar productions in PostgreSQL. This includes functions like XMLCONCAT for concatenating XML values, XMLELEMENT for creating XML elements, XMLPARSE for parsing text into XML, and XMLSERIALIZE for serializing XML to text.

The structure accommodates different XML operations through the op field, with specialized fields for different use cases. The name field carries XML-escaped names, named_args and arg_names work together to represent xml_attribute lists, and args carries other arguments. The type/typmod fields are primarily used for display purposes and may not represent the true result type, which is determined by the XmlExprOp.

## Parameters / Member Variables
- `xpr`: Base expression node containing common expression information
- `op`: XmlExprOp enum value specifying which XML function this represents
- `name`: Name argument for functions like XMLELEMENT and XMLPI (already XML-escaped)
- `named_args`: List of non-XML expressions for xml_attributes functionality
- `arg_names`: Parallel list of String values corresponding to named_args (query_jumble_ignore)
- `args`: List of other expression arguments for the XML function
- `xmloption`: XmlOptionType enum indicating DOCUMENT or CONTENT option (query_jumble_ignore)
- `indent`: Boolean flag for INDENT option in XMLSERIALIZE operations
- `type`: Target data type OID for XMLSERIALIZE (query_jumble_ignore, for display only)
- `typmod`: Type modifier for XMLSERIALIZE (query_jumble_ignore, for display only)
- `location`: Parse location in the original SQL text, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - [XmlExprOp](XmlExprOp.md) (enum)
  - [XmlOptionType](XmlOptionType.md) (enum)
  - ParseLoc
  - [Expr](../E/Expr.md) (base type)
  - [List](../L/List.md)
  - Oid

- Called from (representative examples):
  - [transformXmlExpr](../t/transformXmlExpr.md) (parse_expr.c:2355, 2357, 2361)
  - [transformXmlSerialize](../t/transformXmlSerialize.md) (parse_expr.c:2487, 2491)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (execExpr.c:2260)
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (execExprInterp.c:3888)
  - [get_rule_expr](../g/get_rule_expr.md) (ruleutils.c:9769)

## Notes and Other Information
- [XmlExpr](XmlExpr.md) is part of PostgreSQL's expression node hierarchy, inheriting from the base Expr type
- Multiple fields are marked with pg_node_attr(query_jumble_ignore) to optimize query plan caching by excluding display-only information from query fingerprinting
- The type and typmod fields are used primarily for display purposes and do not necessarily represent the true result type, which is determined by the XmlExprOp
- named_args and arg_names work as parallel lists to represent XML attribute specifications
- The structure supports all major SQL/XML functions including XMLCONCAT, XMLELEMENT, XMLFOREST, XMLPARSE, XMLPI, XMLROOT, XMLSERIALIZE, and IS DOCUMENT
- XML functions require special grammar productions due to their complex syntax with optional parameters and attributes
- The transformation from SQL/XML syntax to this internal representation happens in transformXmlExpr and transformXmlSerialize functions
- Execution is handled by ExecEvalXmlExpr which processes the specific XML operation based on the op field