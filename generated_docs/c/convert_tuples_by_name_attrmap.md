# convert_tuples_by_name_attrmap

## Location
[src/backend/access/common/tupconvert.c:124-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupconvert.c#L124-L153)

## Overview
Creates a tuple conversion map using a pre-built attribute map, providing the core tuple conversion infrastructure setup functionality.

## Definition

```c
structure */
	map = (TupleConversionMap *) palloc(sizeof(TupleConversionMap));
```
## Detailed Description
This function creates and initializes a  structure using a provided attribute map. It serves as the core implementation for tuple conversion setup, taking a pre-built  and combining it with input/output tuple descriptors to create a complete conversion map with preallocated workspace arrays.

The function assumes the attribute map has already been validated and is non-NULL, focusing purely on the mechanical setup of the conversion infrastructure needed for efficient tuple conversion operations.

## Parameters
- : Input tuple descriptor defining the source tuple structure
- : Output tuple descriptor defining the target tuple structure  
- : Pre-built attribute mapping between input and output columns (must be non-NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [TupleConversionMap](../T/TupleConversionMap.md) (struct)
  - [AttrMap](../A/AttrMap.md) (struct)
  - [palloc](../p/palloc.md)
  - Assert
- Called from (representative examples):
  - [convert_tuples_by_name](convert_tuples_by_name.md)
  - [ExecGetRootToChildMap](../E/ExecGetRootToChildMap.md)

## Notes and Other Information
- The function asserts that attrMap is non-NULL, expecting validation to have occurred upstream
- Preallocates workspace arrays (outvalues, outisnull, invalues, inisnull) for efficient conversion
- Position 0 in the invalues/inisnull arrays is reserved for NULL values
- The output workspace is sized based on outdesc->natts, while input workspace includes +1 for NULL
- Memory allocation occurs in the caller's memory context
- The map references the provided descriptors and attribute map, so they must remain valid for the map's lifetime

## Simplified Source

```c
TupleConversionMap *
convert_tuples_by_name_attrmap(TupleDesc indesc,
                               TupleDesc outdesc,
                               AttrMap *attrMap)
{
    TupleConversionMap *map;
    int n = outdesc->natts;

    Assert(attrMap != NULL);

    // Allocate and initialize the conversion map structure
    map = (TupleConversionMap *) palloc(sizeof(TupleConversionMap));
    map->indesc = indesc;
    map->outdesc = outdesc;
    map->attrMap = attrMap;

    // Preallocate output workspace arrays
    map->outvalues = (Datum *) palloc(n * sizeof(Datum));
    map->outisnull = (bool *) palloc(n * sizeof(bool));

    // Preallocate input workspace arrays (+1 for NULL entry)
    n = indesc->natts + 1;
    map->invalues = (Datum *) palloc(n * sizeof(Datum));
    map->inisnull = (bool *) palloc(n * sizeof(bool));

    // Initialize NULL entry at position 0
    map->invalues[0] = (Datum) 0;
    map->inisnull[0] = true;

    return map;
}
```