# make_viewdef

## Location
[src/backend/utils/adt/ruleutils.c:5352-5436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5352-L5436)

## Overview
Reconstructs the SELECT part of a view rewrite rule by extracting rule tuple information and converting it back into readable SQL query text.

## Definition

```c
static void
make_viewdef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
			 int prettyFlags, int wrapColumn)
```
## Detailed Description
The  function is responsible for reconstructing the original SELECT statement that defines a view from its internal rule representation stored in the system catalogs. It extracts rule attributes from a heap tuple representing a rewrite rule, validates that the rule is appropriate for view definition reconstruction (must be a SELECT rule with INSTEAD semantics), and then converts the stored query tree back into readable SQL text.

The function performs several validation checks to ensure the rule represents a valid view definition:
- The event type must be '1' (SELECT)
- The rule must be an INSTEAD rule
- The event qualifier must be '<>' (unconditional)
- The command type must be CMD_SELECT
- There must be exactly one action in the rule

If any validation fails, the function leaves the output buffer empty and returns early. Otherwise, it calls  to generate the actual SQL text and appends a semicolon.

## Parameters / Member Variables
- : StringInfo buffer where the reconstructed SELECT statement will be written
- : HeapTuple containing the rule data from the system catalog
- : TupleDesc describing the structure of the rule tuple
- : Formatting flags controlling pretty-printing of the output
- : Column width for line wrapping in the output

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_fnumber](../S/SPI_fnumber.md)
  - [SPI_getbinval](../S/SPI_getbinval.md)
  - [SPI_getvalue](../S/SPI_getvalue.md)
  - [DatumGetChar](../D/DatumGetChar.md)
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - [stringToNode](../s/stringToNode.md)
  - [get_query_def](../g/get_query_def.md)
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - RelationGetDescr
  - CMD_SELECT
- Called from (representative examples):
  - [pg_get_viewdef_worker](../p/pg_get_viewdef_worker.md)

## Notes and Other Information
This function is part of PostgreSQL's rule system utilities and is specifically designed for view definition reconstruction. It's a static function within ruleutils.c, indicating it's an internal implementation detail not exposed to external modules. The function is careful to validate the rule structure before attempting reconstruction, ensuring that only proper view definition rules are processed. The use of AccessShareLock when opening the relation indicates read-only access for metadata extraction.