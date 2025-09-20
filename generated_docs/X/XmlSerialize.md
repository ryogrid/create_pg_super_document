# XmlSerialize

## Location
[src/include/nodes/parsenodes.h:842-850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L842-L850)

## Overview
XmlSerialize represents the raw parse tree node for the XMLSERIALIZE SQL function, which converts XML values to character string representations with specified formatting options.

## Definition

```c
typedef struct XmlSerialize
{
	NodeTag		type;
	XmlOptionType xmloption;	/* DOCUMENT or CONTENT */
	Node	   *expr;
	TypeName   *typeName;
	bool		indent;			/* [NO] INDENT */
	ParseLoc	location;		/* token location, or -1 if unknown */
} XmlSerialize;
```
## Detailed Description
XmlSerialize represents the parsed form of the XMLSERIALIZE function call in SQL statements. This function is part of PostgreSQL's XML support and is used to convert XML values into textual representations. The structure captures the XML option type (DOCUMENT or CONTENT), the source expression to be serialized, the target data type for the result, and formatting preferences such as indentation. This node exists only in the raw parse tree and is transformed into other node types during parse analysis.

## Parameters / Member Variables
- : NodeTag identifier for this node type
- : Specifies whether to serialize as DOCUMENT or CONTENT (affects XML declaration handling)
- : The expression containing the XML value to be serialized
- : The target data type for the serialized result (typically text or varchar)
- : Boolean flag indicating whether to format the output with indentation
- : Source location in the original SQL text (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - [XmlOptionType](XmlOptionType.md) (enum defining DOCUMENT vs CONTENT serialization)
  - [TypeName](../T/TypeName.md) (structure representing the target type specification)
  - ParseLoc (type for tracking source location)
- Called from (representative examples):
  - [transformExprRecurse](../t/transformExprRecurse.md) (processes expressions during parse analysis)
  - transformXmlSerialize (transforms XMLSERIALIZE into executable form)
  - [exprLocation](../e/exprLocation.md) (determines expression source location)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md) (traverses raw expression trees)

## Notes and Other Information
XmlSerialize is specific to PostgreSQL's XML functionality and appears only in the raw parse tree before being transformed into other node types during semantic analysis. The xmloption field affects how XML declarations and document structure are handled during serialization. The DOCUMENT option preserves complete XML document structure while CONTENT option extracts just the content portion. This structure is part of PostgreSQL's SQL/XML standard compliance features.