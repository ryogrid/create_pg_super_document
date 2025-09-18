# detoast_attr

## Location
[src/backend/access/common/detoast.c:116-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/detoast.c#L116-L204)

## Overview
A public entry point function that retrieves a toasted value from compression or external storage, ensuring the result is always in non-extended varlena format.

## Definition


## Detailed Description
This function provides complete detoasting functionality by handling all possible forms of extended varlena values and converting them to standard, non-extended format. It processes:

1. **External on-disk storage**: Fetches data from TOAST relations and decompresses if needed
2. **Indirect pointers**: Dereferences indirect references and recursively processes the result
3. **Expanded objects**: Converts expanded objects to flat format using detoast_external_attr
4. **Compressed values**: Decompresses compressed data within the main tuple
5. **Short-header values**: Converts short-header format to standard 4-byte header format

The function ensures that the returned datum is always in standard varlena format without any extended attributes (compression, external storage, short headers, etc.). This makes it safe for general use where standard format is required.

## Parameters / Member Variables
- : A pointer to the varlena structure that may be in extended format and needs to be converted to standard format

## Dependencies
- Functions called/Symbols referenced:
  - [toast_fetch_datum](../t/toast_fetch_datum.md): Retrieves data from TOAST relations for external values
  - [toast_decompress_datum](../t/toast_decompress_datum.md): Decompresses compressed varlena data
  - [detoast_external_attr](detoast_external_attr.md): Handles externally stored attributes
  - VARATT_IS_EXTERNAL_ONDISK: Macro to check if value is stored externally on disk
  - VARATT_IS_EXTERNAL_INDIRECT: Macro to check if value is an indirect pointer
  - VARATT_IS_EXTERNAL_EXPANDED: Macro to check if value is an expanded object
  - VARATT_IS_COMPRESSED: Macro to check if value is compressed
  - VARATT_IS_SHORT: Macro to check if value uses short header format
  - VARATT_IS_EXTENDED: Macro to check if value has any extended attributes
  - VARATT_EXTERNAL_GET_POINTER: Extracts pointer from indirect reference
  - VARSIZE_SHORT: Gets size of short-header varlena
  - VARHDRSZ_SHORT: Size of short header
  - SET_VARSIZE: Sets the size field of a varlena
  - VARDATA/VARDATA_SHORT: Macros to access varlena data
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
- Called from (representative examples):
  - [pg_detoast_datum](../p/pg_detoast_datum.md): Main detoasting interface in function manager
  - [pg_detoast_datum_copy](../p/pg_detoast_datum_copy.md): Copy-based detoasting variant
  - [pg_detoast_datum_packed](../p/pg_detoast_datum_packed.md): Packed detoasting variant
  - [toast_flatten_tuple_to_datum](../t/toast_flatten_tuple_to_datum.md): Tuple flattening operations

## Notes and Other Information
- The function guarantees that the result is in non-extended varlena format, making it suitable for operations that require standard format
- For indirect pointers, the function recursively calls itself and copies the result if no processing was needed
- The function includes assertions to ensure that flatteners don't produce compressed or short output
- Short-header values are converted to standard 4-byte header format by allocating new memory and copying data
- This function is commonly used through the pg_detoast_datum family of functions in the function manager
- Essential component of PostgreSQL's TOAST system for handling oversized attributes