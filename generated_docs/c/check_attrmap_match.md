# check_attrmap_match

## Location
[src/backend/access/common/attmap.c:290-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/attmap.c#L290-L329)

## Overview
Determines whether an attribute map represents a one-to-one match between input and output tuple descriptors, enabling optimization by skipping tuple conversion when possible.

## Definition
static bool check_attrmap_match(TupleDesc indesc, TupleDesc outdesc, AttrMap *attrMap)

## Detailed Description
This function analyzes an attribute map to determine if it represents a simple one-to-one correspondence between input and output tuple descriptors. When such a match exists, PostgreSQL can optimize operations by avoiding the overhead of tuple conversion, as the data can be used directly without transformation.

The function performs several checks to ensure that the attribute mapping is truly equivalent:
1. Verifies that both tuple descriptors have the same number of attributes
2. Examines each attribute position to confirm it maps to the same position in the output
3. Handles special cases for dropped columns and missing attributes
4. Ensures that dropped columns in both descriptors have compatible storage properties

This optimization is particularly important in scenarios involving table inheritance, partition pruning, and view operations where tuple structures may be logically equivalent but require verification.

## Parameters / Member Variables
- : Input tuple descriptor containing the source attribute definitions
- : Output tuple descriptor containing the target attribute definitions  
- : Attribute mapping structure that defines the correspondence between input and output attributes

## Dependencies
- Functions called/Symbols referenced:
  - [AttrMap](../A/AttrMap.md)
  - TupleDescAttr
  - Form_pg_attribute

- Called from (representative examples):
  - [build_attrmap_by_position](../b/build_attrmap_by_position.md)
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)

## Notes and Other Information
- Returns false immediately if input and output descriptors have different numbers of attributes
- Special handling for missing attributes (atthasmissing flag) - always requires conversion
- Dropped columns are allowed to match if both input and output are dropped and have compatible storage alignment (attlen and attalign)
- The function assumes attribute numbering starts from 1 (hence the i+1 comparison)
- This is a static function within the attmap.c module, used internally for attribute mapping optimization
- Performance-critical function as it determines whether expensive tuple conversion can be avoided

## Simplified Source

```c
static bool
check_attrmap_match(TupleDesc indesc, TupleDesc outdesc, AttrMap *attrMap)
{
    int i;

    // Quick check: attribute counts must match
    if (indesc->natts != outdesc->natts)
        return false;

    // Check each attribute position
    for (i = 0; i < attrMap->maplen; i++) {
        Form_pg_attribute inatt = TupleDescAttr(indesc, i);
        Form_pg_attribute outatt = TupleDescAttr(outdesc, i);

        // Missing attributes always require conversion
        if (inatt->atthasmissing)
            return false;

        // Check if attribute maps to same position (1-based numbering)
        if (attrMap->attnums[i] == (i + 1))
            continue;

        // Special case: both attributes are dropped with compatible alignment
        if (attrMap->attnums[i] == 0 &&
            inatt->attisdropped &&
            inatt->attlen == outatt->attlen &&
            inatt->attalign == outatt->attalign)
            continue;

        // Any other mapping difference means no match
        return false;
    }

    return true;
}
```