# _outConst

## Location
[src/backend/nodes/outfuncs.c:382-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L382-L401)

## Overview
Serializes a Const node to its string representation, outputting all metadata fields and the actual constant value using outDatum for complete node information.

## Definition

```c
enum representation */
	switch (node->boolop)
	{
		case AND_EXPR:
			opstr = "and";
			break;
		case OR_EXPR:
			opstr = "or";
			break;
		case NOT_EXPR:
			opstr = "not";
			break;
	}
	appendStringInfoString(str, " :boolop ");
```
## Detailed Description
The  function is a specialized node serialization function that handles Const nodes in PostgreSQL's expression tree system. Const nodes represent literal constant values in SQL queries (like numbers, strings, booleans, etc.).

The function outputs all essential metadata about the constant including its data type information (consttype, consttypmod, constcollid), storage characteristics (constlen, constbyval), null status (constisnull), and source location. For non-null constants, it delegates to  to serialize the actual constant value in raw byte format.

This function follows PostgreSQL's standard node output conventions using the WRITE_* macros for consistent formatting and is part of the custom_read_write or special_read_write attribute system for node serialization.

## Parameters / Member Variables
- : StringInfo buffer where the serialized output is appended
- : Pointer to the Const node to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - WRITE_NODE_TYPE
  - WRITE_OID_FIELD  
  - WRITE_INT_FIELD
  - WRITE_BOOL_FIELD
  - WRITE_LOCATION_FIELD
  - [outDatum](outDatum.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
- Called from (representative examples):
  - (Part of node output dispatch system - called indirectly through nodeToString mechanisms)

## Notes and Other Information
This function is part of PostgreSQL's node serialization infrastructure and is primarily used for debugging, EXPLAIN output, and internal query plan representation. The distinction between null and non-null constants is handled explicitly - null constants output "<>" while non-null constants use outDatum to show the raw byte representation. The function is marked static, indicating it's an internal helper within the outfuncs.c module and accessed through function pointer dispatch tables for node type-specific serialization.

## Simplified Source

```c
static void
_outConst(StringInfo str, const Const *node)
{
    // Write node type identifier
    WRITE_NODE_TYPE("CONST");

    // Output all metadata fields
    WRITE_OID_FIELD(consttype);
    WRITE_INT_FIELD(consttypmod);
    WRITE_OID_FIELD(constcollid);
    WRITE_INT_FIELD(constlen);
    WRITE_BOOL_FIELD(constbyval);
    WRITE_BOOL_FIELD(constisnull);
    WRITE_LOCATION_FIELD(location);

    // Output the actual constant value
    appendStringInfoString(str, " :constvalue ");
    if (node->constisnull)
        appendStringInfoString(str, "<>");
    else
        outDatum(str, node->constvalue, node->constlen, node->constbyval);
}
```