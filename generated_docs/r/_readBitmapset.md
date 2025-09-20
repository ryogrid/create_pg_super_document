# _readBitmapset

## Location
[src/backend/nodes/readfuncs.c:203-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/readfuncs.c#L203-L244)

## Overview
A static helper function in PostgreSQL's node deserialization system that parses serialized Bitmapset structures from text format, reconstructing the original Bitmapset data structure with all its member integers.

## Definition
```c
static Bitmapset *_readBitmapset(void)
```

## Detailed Description
The `_readBitmapset` function is responsible for deserializing Bitmapset objects from their textual representation during node reading operations. It follows a specific parsing protocol:

1. **Structure validation**: Expects the token sequence to start with "(" followed by "b" to identify a Bitmapset structure
2. **Member parsing**: Iteratively reads integer values and adds them to the Bitmapset using `bms_add_member`
3. **Termination**: Continues reading until it encounters the closing ")" token
4. **Error handling**: Provides detailed error messages for malformed structures, incomplete data, or invalid integer values

The function uses PostgreSQL's standard tokenization mechanism (`pg_strtok`) and follows the node system's conventions for structure parsing. It's designed to work in contexts where a Bitmapset is specifically expected, as opposed to the more general `nodeRead()` function.

## Parameters / Member Variables
- Returns: `Bitmapset *` - pointer to the reconstructed Bitmapset, or NULL if no members were found

## Dependencies
- Functions called/Symbols referenced:
  - `READ_TEMP_LOCALS` (macro for local tokenization variables)
  - [pg_strtok](../p/pg_strtok.md) (tokenization function)
  - `strtol` (string to integer conversion)
  - [bms_add_member](../b/bms_add_member.md) (adds member to Bitmapset)
  - `elog` (error logging)
- Called from (representative examples):
  - `READ_BITMAPSET_FIELD` (macro for reading Bitmapset fields)
  - [readBitmapset](readBitmapset.md) (public wrapper function)

## Notes and Other Information
- This is a static function, only accessible within the readfuncs.c compilation unit
- Expects input format: "( b integer1 integer2 ... integerN )"
- Uses strict error checking with `elog(ERROR, ...)` for any parsing failures
- Companion to `_outBitmapset` in outfuncs.c for serialization
- Part of PostgreSQL's broader node system used extensively in query planning and execution
- The function builds the Bitmapset incrementally, starting with NULL and adding members one by one