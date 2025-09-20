# ECPGd_cardinality

## Location
[src/interfaces/ecpg/include/ecpgtype.h:89-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/include/ecpgtype.h#L89-L91)

## Overview
ECPGd_cardinality is an enumeration constant that represents the cardinality descriptor item within the ECPG (Embedded SQL in C for PostgreSQL) descriptor type system.

## Definition

```c
enum ECPG_statement_type
{
	ECPGst_normal,
	ECPGst_execute,
	ECPGst_exec_immediate,
	ECPGst_prepnormal,
	ECPGst_prepare,
	ECPGst_exec_with_exprlist
};
```
## Detailed Description
ECPGd_cardinality is the final enumeration value in the ECPGdtype enum, defined in src/interfaces/ecpg/include/ecpgtype.h:89. It represents the cardinality attribute of SQL descriptors in the ECPG system. Cardinality refers to the number of elements or rows that a particular descriptor item can contain or process.

This enumeration constant is used within the ECPG descriptor handling system to identify and manipulate cardinality information when working with SQL descriptors. SQL descriptors are data structures that contain metadata about SQL statements, including information about parameters, result sets, and their characteristics.

The placement of ECPGd_cardinality after ECPGd_EODT (End of descriptor types) suggests it may be a special or extended descriptor type that is handled differently from the standard descriptor items.

## Parameters / Member Variables
As an enumeration constant, ECPGd_cardinality has no parameters or member variables. It serves as a symbolic identifier used throughout the ECPG descriptor system.

## Dependencies
- Functions called/Symbols referenced: None (enumeration constant)
- Used by (representative examples):
  - [ECPGget_desc](ECPGget_desc.md) (src/interfaces/ecpg/ecpglib/descriptor.c:409)
  - [descriptor_item_name](../d/descriptor_item_name.md) (src/interfaces/ecpg/preproc/descriptor.c:237)
  - [output_set_descr](../o/output_set_descr.md) (src/interfaces/ecpg/preproc/descriptor.c:286)
  - [get_dtype](../g/get_dtype.md) (src/interfaces/ecpg/preproc/type.c:741)

## Notes and Other Information
- [ECPGd_cardinality](ECPGd_cardinality.md) appears after ECPGd_EODT in the enumeration, suggesting it may be treated as a special case or extension beyond the standard descriptor types
- The cardinality concept is important in SQL for understanding the size and scope of data operations, particularly relevant for array operations and bulk data processing
- Usage in descriptor-related functions indicates its role in SQL descriptor metadata management
- The symbol is used in both runtime library functions (ecpglib) and preprocessor functions (preproc), showing its importance across different phases of ECPG processing
- Cardinality information is crucial for memory allocation and iteration when processing SQL results or parameters in embedded SQL applications