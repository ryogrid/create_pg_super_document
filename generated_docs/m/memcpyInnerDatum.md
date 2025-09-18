# memcpyInnerDatum

## Location
[src/backend/access/spgist/spgutils.c:789-809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L789-L809)

## Overview
Copies a non-null datum to a target memory location for storage in an SP-GiST inner tuple, handling both pass-by-value and pass-by-reference data types appropriately.

## Definition


## Detailed Description
This static function performs type-aware copying of datum values into inner tuple storage. It implements the storage convention where pass-by-value types are stored as their Datum representation directly, while pass-by-reference types are stored as their actual data content. The function handles both fixed-length and variable-length pass-by-reference types correctly.

For pass-by-value types, it copies the Datum value itself (which contains the actual data). For pass-by-reference types, it dereferences the pointer and copies the actual data content, determining the correct size based on whether the type has a fixed length or is variable-length.

## Parameters / Member Variables
- : Pointer to the destination memory location where the datum should be copied
- : Pointer to SpGistTypeDesc structure containing type information (byval flag, length, etc.)
- : The source datum value to be copied

## Dependencies
- Functions called/Symbols referenced:
  - SpGistTypeDesc (type descriptor structure)
  - VARSIZE_ANY (macro for getting variable-length type size)
  - [DatumGetPointer](../D/DatumGetPointer.md) (macro to extract pointer from datum)
  - memcpy (standard memory copy function)
- Called from (representative examples):
  - [spgFormNodeTuple](../s/spgFormNodeTuple.md)
  - [spgFormInnerTuple](../s/spgFormInnerTuple.md)

## Notes and Other Information
- This is a static function, only accessible within the spgutils.c file
- The function assumes the datum is non-null (as stated in the comment)
- The copying strategy is designed to work in conjunction with SpGistGetInnerTypeSize for consistent storage layout
- For pass-by-value types, the entire Datum (typically 8 bytes on 64-bit systems) is copied regardless of the actual data size
- This function is part of the SP-GiST access method's tuple formation infrastructure