# transformXmlExpr

## Location
[src/backend/parser/parse_expr.c:2355-2483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2355-L2483)

## Overview
Transforms XML expression nodes during parsing by converting raw XML expressions into properly typed and validated expressions with appropriate type coercion for different XML operations.

## Definition

```c
static Node *
transformXmlExpr(ParseState *pstate, XmlExpr *x)
```
## Detailed Description
The  function is responsible for transforming XML expressions during the parsing phase. It creates a new XmlExpr node from the input, handling named arguments, argument names, and applying appropriate type coercion based on the specific XML operation type (XMLCONCAT, XMLELEMENT, XMLFOREST, XMLPARSE, XMLPI, XMLROOT, IS_DOCUMENT).

The function processes named arguments by transforming ResTarget nodes, extracting argument names, and validating them. For XMLELEMENT operations, it checks for duplicate attribute names. It then processes regular arguments with operation-specific type coercion - for example, XMLCONCAT and XMLFOREST arguments are coerced to XML type, while XMLPARSE arguments require TEXT for the first argument and BOOLEAN for subsequent ones.

## Parameters / Member Variables
- : ParseState context for the current parsing operation
- : Input XmlExpr node to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [map_sql_identifier_to_xml_name](../m/map_sql_identifier_to_xml_name.md)
  - [transformExprRecurse](transformExprRecurse.md)
  - [FigureColname](../F/FigureColname.md)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - [coerce_to_boolean](../c/coerce_to_boolean.md)
  - [makeString](../m/makeString.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- The function sets the output type to XMLOID to mark the node as transformed
- XMLSERIALIZE operations are not handled in this function (assertion failure occurs)
- Named argument processing includes validation for duplicate attribute names in XMLELEMENT
- Different XML operations require different type coercions for their arguments
- The function is located in src/backend/parser/parse_expr.c:2355-2483

## Simplified Source

```c
static Node *
transformXmlExpr(ParseState *pstate, XmlExpr *x)
{
    XmlExpr *newx = makeNode(XmlExpr);
    ListCell *lc;
    int i;

    // Copy basic fields
    newx->op = x->op;
    newx->name = x->name ? map_sql_identifier_to_xml_name(x->name, false, false) : NULL;
    newx->xmloption = x->xmloption;
    newx->type = XMLOID;  // Mark as transformed
    newx->typmod = -1;
    newx->location = x->location;

    // Process named arguments (for attributes/elements)
    newx->named_args = NIL;
    newx->arg_names = NIL;

    foreach(lc, x->named_args) {
        ResTarget *r = lfirst_node(ResTarget, lc);
        Node *expr = transformExprRecurse(pstate, r->val);
        char *argname;

        // Determine argument name
        if (r->name) {
            argname = map_sql_identifier_to_xml_name(r->name, false, false);
        } else if (IsA(r->val, ColumnRef)) {
            argname = map_sql_identifier_to_xml_name(FigureColname(r->val), true, false);
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("unnamed XML element value must be a column reference")));
        }

        // Check for duplicate attribute names in XMLELEMENT
        if (x->op == IS_XMLELEMENT) {
            foreach(lc2, newx->arg_names) {
                if (strcmp(argname, strVal(lfirst(lc2))) == 0)
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                   errmsg("XML attribute name \"%s\" appears more than once", argname)));
            }
        }

        newx->named_args = lappend(newx->named_args, expr);
        newx->arg_names = lappend(newx->arg_names, makeString(argname));
    }

    // Process regular arguments with type coercion
    newx->args = NIL;
    i = 0;
    foreach(lc, x->args) {
        Node *e = (Node *) lfirst(lc);
        Node *newe = transformExprRecurse(pstate, e);

        // Apply operation-specific type coercion
        switch (x->op) {
            case IS_XMLCONCAT:
            case IS_XMLFOREST:
                newe = coerce_to_specific_type(pstate, newe, XMLOID, "XMLCONCAT");
                break;
            case IS_XMLPARSE:
                if (i == 0)
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLPARSE");
                else
                    newe = coerce_to_boolean(pstate, newe, "XMLPARSE");
                break;
            case IS_XMLPI:
                newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLPI");
                break;
            case IS_XMLROOT:
                if (i == 0)
                    newe = coerce_to_specific_type(pstate, newe, XMLOID, "XMLROOT");
                else if (i == 1)
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLROOT");
                else
                    newe = coerce_to_specific_type(pstate, newe, INT4OID, "XMLROOT");
                break;
            case IS_DOCUMENT:
                newe = coerce_to_specific_type(pstate, newe, XMLOID, "IS DOCUMENT");
                break;
        }

        newx->args = lappend(newx->args, newe);
        i++;
    }

    return (Node *) newx;
}
```