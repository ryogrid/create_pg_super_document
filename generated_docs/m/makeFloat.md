# makeFloat

## Location
[src/backend/nodes/value.c:37-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/value.c#L37-L48)

## Overview
The makeFloat function creates a new Float node containing a string representation of a numeric value, used for representing floating-point and large integer literals in PostgreSQL's parse tree structure.

## Definition
Float *makeFloat(char *numericStr)

## Detailed Description
makeFloat is a factory function that allocates and initializes a new Float node in PostgreSQL's node system. Unlike typical float implementations, PostgreSQL stores Float nodes as strings rather than native C doubles to preserve precision during parsing and avoid potential precision loss that could occur with binary floating-point representation.

The Float node is used to represent numeric literals that either contain decimal points or are integers too large to fit in a standard int type. This string-based approach allows PostgreSQL to maintain exact precision until the value is finally converted to its target numeric type (which might be NUMERIC, double precision, etc.) during later processing stages.

The caller is responsible for ensuring that the numericStr parameter is a palloc'd (PostgreSQL-allocated) string, as the Float node will take ownership of this memory.

## Parameters / Member Variables
- `numericStr`: A palloc'd string containing the string representation of the numeric value. The caller must ensure this memory is allocated with palloc().

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation and initialization)
  - [Float](../F/Float.md) (struct type definition)
- Called from (representative examples):
  - [pg_get_object_address](../p/pg_get_object_address.md) (in objectaddress.c)
  - [sequence_options](../s/sequence_options.md) (in sequence.c, multiple locations)
  - [buildDefItem](../b/buildDefItem.md) (in tsearchcmds.c)
  - [nodeRead](../n/nodeRead.md) (in read.c for deserialization)

## Notes and Other Information
- Stores numeric values as strings internally rather than binary floating-point to preserve precision
- Used for both floating-point literals and integers too large for the int type
- The caller must provide a palloc'd string - the function does not copy the string
- Part of PostgreSQL's value node system alongside makeInteger, makeString, and makeBitString
- Critical for maintaining numeric precision during parsing and early processing stages
- The string representation allows for later conversion to appropriate PostgreSQL numeric types without precision loss

## Simplified Source

```c
Float *makeFloat(char *numericStr)
{
    Float *v = makeNode(Float);
    v->fval = numericStr;
    return v;
}
```