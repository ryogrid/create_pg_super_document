# AttrDefault

## Location
[src/include/access/tupdesc.h:22-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tupdesc.h#L22-L26)

## Overview
AttrDefault represents a column default value constraint in PostgreSQL's tuple descriptor system, storing both the column number and the serialized expression for default values.

## Definition

```c
typedef struct AttrDefault
{
	AttrNumber	adnum;
	char	   *adbin;			/* nodeToString representation of expr */
} AttrDefault;
```
## Detailed Description
AttrDefault is a structure that stores information about default values for table columns. It is part of PostgreSQL's constraint system and is used within tuple descriptors to maintain default value information. The structure contains the column number (attribute number) and a string representation of the default value expression that has been serialized using nodeToString().

This structure is primarily used during table creation, constraint management, and when evaluating default values for INSERT operations. The serialized expression format allows for complex default expressions to be stored and later reconstructed for evaluation.

## Parameters / Member Variables
- : AttrNumber identifying which column this default applies to (1-based indexing)
- : String containing the nodeToString representation of the default value expression

## Dependencies
- Functions called/Symbols referenced:
  - AttrNumber (type)
- Called from (representative examples):
  - [CreateTupleDescCopyConstr](../C/CreateTupleDescCopyConstr.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - [equalTupleDescs](../e/equalTupleDescs.md)
  - [TupleDescGetDefault](../T/TupleDescGetDefault.md)
  - [AttrDefaultFetch](AttrDefaultFetch.md)
  - [AttrDefaultCmp](AttrDefaultCmp.md)

## Notes and Other Information
- The adbin field stores expressions in a serialized format that can be reconstructed using stringToNode()
- [AttrDefault](AttrDefault.md) structures are typically stored in arrays within TupleConstr
- Used extensively in the relation cache system for managing table constraints
- The structure is designed to be lightweight while preserving all necessary information about column defaults