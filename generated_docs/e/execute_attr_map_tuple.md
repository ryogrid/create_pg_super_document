# execute_attr_map_tuple

## Location
[src/backend/access/common/tupconvert.c:154-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupconvert.c#L154-L191)

## Overview
Performs actual tuple conversion according to a pre-built tuple conversion map, transforming a HeapTuple from input format to output format.

## Definition

```c
HeapTuple
execute_attr_map_tuple(HeapTuple tuple, TupleConversionMap *map)
```
## Detailed Description
This function performs the actual work of converting a tuple from one format to another using a pre-built . It extracts all values from the input tuple, transposes them according to the attribute mapping, and constructs a new tuple in the target format.

The conversion process involves three main steps: 1) decomposing the input tuple into Datum/null arrays using , 2) mapping the values according to the attribute map by copying data from input positions to output positions, and 3) forming the new tuple with .

## Parameters
- : Input HeapTuple to be converted
- : Pre-built TupleConversionMap containing conversion instructions and workspace arrays

## Dependencies
- Functions called/Symbols referenced:
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - TupleConversionMap (struct)
  - [AttrMap](../A/AttrMap.md) (struct)
  - Assert
- Called from (representative examples):
  - [acquire_inherited_sample_rows](../a/acquire_inherited_sample_rows.md)
  - [ExecEvalConvertRowtype](../E/ExecEvalConvertRowtype.md)

## Notes and Other Information
- Uses the preallocated workspace arrays (invalues, inisnull, outvalues, outisnull) from the conversion map for efficiency
- The invalues array is offset by +1 so that invalues[0] remains NULL and invalues[1] corresponds to the first source attribute
- This indexing convention exactly matches the numbering in the attribute map (attnums array)
- The function asserts that the attribute map length matches the output descriptor's attribute count
- Returns a newly allocated HeapTuple that must be managed by the caller
- Efficient for repeated conversions as workspace arrays are reused