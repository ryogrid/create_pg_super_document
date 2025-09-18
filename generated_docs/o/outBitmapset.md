# outBitmapset

## Location
[src/backend/nodes/outfuncs.c:325-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L325-L340)

## Overview
Converts a PostgreSQL Bitmapset structure into a parenthesized string format with a 'b' type indicator, outputting all set member integers in ascending order.

## Definition
void outBitmapset(StringInfo str, const Bitmapset *bms)

## Detailed Description
The outBitmapset function serializes PostgreSQL's Bitmapset data structure, which represents a set of non-negative integers using an efficient bitmap representation. The output format follows the pattern "(b int int ...)", similar to an integer list but with a distinct 'b' type indicator to differentiate it from other list types.

The function iterates through all members of the bitmapset in ascending order using the bms_next_member() utility function. Starting with x = -1, each call to bms_next_member() returns the next member in the set that is greater than the current value, or -1 when no more members exist. This approach ensures that the integers are output in sorted order, which provides consistency and predictability in the serialized format.

The function is explicitly exported (not declared static) to allow PostgreSQL extensions that define extensible nodes to use it directly. However, the comment notes that this is somewhat historical, as calling the generic outNode() function will also work for Bitmapset structures.

The serialized format is designed to be efficiently parsed back into a Bitmapset structure by the corresponding reading functions, maintaining the complete set membership information.

## Parameters / Member Variables
- `str`: StringInfo buffer where the serialized bitmapset will be appended
- `bms`: Const pointer to the Bitmapset structure to be serialized (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoChar (for outputting parentheses and type indicator)
  - [bms_next_member](../b/bms_next_member.md) (utility function for iterating through bitmapset members)
  - appendStringInfo (for formatted output of integer members)

- Called from (representative examples):
  - WRITE_BITMAPSET_FIELD (macro in outfuncs.c:104)
  - outNode (main node output dispatcher in outfuncs.c:738)
  - [bmsToString](../b/bmsToString.md) (utility function in outfuncs.c:814)

## Notes and Other Information
- This function is exported (public) unlike most other out* functions, allowing use by extensions
- Handles NULL bitmapsets gracefully (bms_next_member handles NULL input)
- Output format maintains sorted order of integers for consistency
- The 'b' type indicator allows parsers to distinguish bitmapsets from other list-like structures
- Part of PostgreSQL's efficient representation for sets of integers, commonly used for representing column sets, relation sets, and other discrete collections
- The iteration pattern with bms_next_member() is the standard way to traverse bitmapset contents
- Maintains compatibility with the broader node serialization/deserialization infrastructure