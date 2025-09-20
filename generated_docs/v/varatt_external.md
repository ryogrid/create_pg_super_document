# varatt_external

## Location
[src/include/varatt.h:32-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/varatt.h#L32-L39)

## Overview
A structure representing a traditional "TOAST pointer" that contains the information needed to fetch a Datum stored out-of-line in a TOAST table.

## Definition

```c
typedef struct varatt_external
{
	int32		va_rawsize;		/* Original data size (includes header) */
	uint32		va_extinfo;		/* External saved size (without header) and
								 * compression method */
	Oid			va_valueid;		/* Unique ID of value within TOAST table */
	Oid			va_toastrelid;	/* RelID of TOAST table containing it */
}			varatt_external;
```
## Detailed Description
The varatt_external structure is a fundamental component of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system. It serves as a pointer to data that has been stored out-of-line in a separate TOAST table due to its large size. The structure contains all the necessary information to locate and retrieve the original data from the TOAST table.

The data referenced by this pointer is compressed if and only if the external size stored in va_extinfo is less than va_rawsize - VARHDRSZ. This structure is designed to be stored unaligned within actual tuples and must not contain any padding to ensure consistency when using memcmp for equality comparisons.

## Parameters / Member Variables
- `va_rawsize`: The original size of the data including the header, representing the full size of the uncompressed data
- `va_extinfo`: Contains both the external saved size (without header) and compression method information
- `va_valueid`: A unique identifier for the value within the TOAST table, used to locate the specific data
- `va_toastrelid`: The relation ID of the TOAST table that contains the out-of-line data
## Dependencies
- Functions called/Symbols referenced:
  - int32 (PostgreSQL type)
  - uint32 (PostgreSQL type)
  - Oid (PostgreSQL type)
- Called from (representative examples):
  - [detoast_attr_slice](../d/detoast_attr_slice.md)
  - [toast_fetch_datum](../t/toast_fetch_datum.md)
  - [toast_fetch_datum_slice](../t/toast_fetch_datum_slice.md)
  - [toast_save_datum](../t/toast_save_datum.md)
  - [toast_delete_datum](../t/toast_delete_datum.md)
  - [ReorderBufferToastReplace](../R/ReorderBufferToastReplace.md)

## Notes and Other Information
- This structure must not contain padding to ensure memcmp compatibility
- Data is stored unaligned within tuples, requiring memcpy to access fields safely
- Compression detection is performed by comparing va_extinfo with (va_rawsize - VARHDRSZ)
- Used extensively throughout PostgreSQL's TOAST system for managing large attribute values
- Critical for the out-of-line storage mechanism that allows PostgreSQL to handle very large data values efficiently