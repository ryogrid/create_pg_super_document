# ECPGt_bytea

## Location
[src/interfaces/ecpg/include/ecpgtype.h:67-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/include/ecpgtype.h#L67-L70)

## Overview
ECPGt_bytea is an enumeration constant that represents the PostgreSQL bytea data type within the ECPG (Embedded SQL in C for PostgreSQL) type system.

## Definition

```c
enum ECPGdtype
{
	ECPGd_count = 1,
	ECPGd_data,
	ECPGd_di_code,
	ECPGd_di_precision,
	ECPGd_indicator,
	ECPGd_key_member,
	ECPGd_length,
	ECPGd_name,
	ECPGd_nullable,
	ECPGd_octet,
	ECPGd_precision,
	ECPGd_ret_length,
	ECPGd_ret_octet,
	ECPGd_scale,
	ECPGd_type,
	ECPGd_EODT,					/* End of descriptor types. */
	ECPGd_cardinality
};
```
## Detailed Description
ECPGt_bytea is the final enumeration value in the ECPGttype enum, defined in src/interfaces/ecpg/include/ecpgtype.h:67. It represents the bytea data type, which is used to store binary data in PostgreSQL. This enumeration constant is used throughout the ECPG system to identify and handle bytea data types when processing embedded SQL statements in C programs.

The bytea type is classified as a simple type within the ECPG system, as evidenced by its inclusion in the IS_SIMPLE_TYPE macro definition. This means it can be handled directly without complex processing or conversion routines that are required for composite types like arrays or structures.

## Parameters / Member Variables
As an enumeration constant, ECPGt_bytea has no parameters or member variables. It serves as a symbolic identifier with an integer value used throughout the ECPG system.

## Dependencies
- Functions called/Symbols referenced: None (enumeration constant)
- Used by (representative examples):
  - [ecpg_get_data](../e/ecpg_get_data.md) (src/interfaces/ecpg/ecpglib/data.c:521)
  - [set_desc_attr](../s/set_desc_attr.md) (src/interfaces/ecpg/ecpglib/descriptor.c:587)
  - [ecpg_store_input](../e/ecpg_store_input.md) (src/interfaces/ecpg/ecpglib/execute.c:820)
  - [ecpg_build_params](../e/ecpg_build_params.md) (src/interfaces/ecpg/ecpglib/execute.c:1398)
  - IS_SIMPLE_TYPE (src/interfaces/ecpg/include/ecpgtype.h:92)
  - [get_type](../g/get_type.md) (src/interfaces/ecpg/preproc/type.c:178)

## Notes and Other Information
- [ECPGt_bytea](ECPGt_bytea.md) is included in the IS_SIMPLE_TYPE macro, indicating that bytea data is handled as a simple type rather than a complex type requiring special processing
- The symbol is extensively used in ECPG test cases, particularly in sql-bytea.c test file, demonstrating its importance in binary data handling
- As the last enumeration value in ECPGttype, it represents one of the most recently added or specialized data types in the ECPG type system
- The bytea type is essential for applications that need to store and retrieve binary data such as images, files, or encrypted data through embedded SQL