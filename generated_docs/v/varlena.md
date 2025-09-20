# varlena

## Location
[src/include/c.h:686-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/c.h#L686-L691)

## Overview
The  structure is the fundamental header for all variable-length datatypes in PostgreSQL, providing a unified interface for storing and managing variable-length data including strings, bytea, and other complex types.

## Definition

```c
struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};
```
## Detailed Description
The  structure serves as the universal header for all variable-length datatypes in PostgreSQL. It implements a sophisticated storage system that supports both inline and out-of-line (TOASTed) storage for large values. The structure is designed to be memory-efficient while providing flexibility for handling values of varying sizes.

Key characteristics:
- **Universal Header**: All variable-length types (text, bytea, varchar, etc.) use this structure
- **TOAST Support**: Handles compressed and out-of-line storage for large values
- **Length Encoding**: The  field uses a complex encoding scheme to store both length and type information
- **Flexible Array**: Uses  for variable-length data storage

The structure is intentionally designed to discourage direct field access, instead promoting the use of specialized macros for safe manipulation.

## Parameters / Member Variables
- : A 4-byte field that encodes the total size of the varlena structure and additional metadata. **Direct access is strongly discouraged** due to complex encoding schemes for TOAST support
- : The actual data content stored as a flexible array member, allowing for variable-length data storage

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [detoast_attr](../d/detoast_attr.md)
  - [toast_compress_datum](../t/toast_compress_datum.md)
  - [toast_save_datum](../t/toast_save_datum.md)
  - [hashvarlena](../h/hashvarlena.md)
  - [pg_detoast_datum](../p/pg_detoast_datum.md)
  - [bytea](../b/bytea.md)
  - [text](../t/text.md)
  - [BpChar](../B/BpChar.md)
  - [VarChar](../V/VarChar.md)

## Notes and Other Information
- **TOAST Integration**: The structure is deeply integrated with PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system for handling large values
- **Macro Usage**: Always use the provided macros (VARDATA_ANY, VARSIZE_ANY, VARDATA, VARSIZE, SET_VARSIZE) instead of direct field access
- **Type Safety**: The structure provides type safety for variable-length data manipulation across the entire PostgreSQL codebase
- **Memory Layout**: Designed for efficient memory usage with minimal overhead for small values
- **Compression Support**: Supports various compression methods through the TOAST system
- **Cross-Platform**: The 4-byte length field ensures consistent behavior across different architectures