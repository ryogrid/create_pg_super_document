# sqlda_variable

## Location
[src/interfaces/ecpg/preproc/descriptor.c:351-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L351-L366)

## Overview
Creates and returns a dynamically allocated variable structure for SQLDA (SQL Descriptor Area) variables used in ECPG preprocessing.

## Definition
```c
struct variable *sqlda_variable(const char *name)
```

## Detailed Description
The `sqlda_variable` function creates a new variable structure specifically for SQLDA (SQL Descriptor Area) variables in ECPG. Unlike `descriptor_variable` which uses static storage, this function dynamically allocates memory for both the variable structure and its associated type information. SQLDA is a standard interface for dynamic SQL that allows programs to work with result sets whose structure is not known at compile time.

The function performs the following operations:
1. Allocates memory for a new variable structure
2. Duplicates the provided name string and assigns it to the variable
3. Allocates and initializes an ECPGtype structure with type ECPGt_sqlda
4. Sets all type-specific fields to appropriate default values
5. Returns the fully initialized variable structure

## Parameters / Member Variables
- `name`: The name of the SQLDA variable to create

## Dependencies
- Functions called/Symbols referenced:
  - [mm_alloc](../m/mm_alloc.md) (memory allocation function)
  - [mm_strdup](../m/mm_strdup.md) (string duplication function)
  - [ECPGtype](../E/ECPGtype.md)
  - ECPGt_sqlda
  - [element](../e/element.md)
- Called from (representative examples):
  - Used in ecpg.trailer for handling SQLDA variables in dynamic SQL statements
  - Referenced in ECPG grammar processing for SQLDA operations

## Notes and Other Information
- Uses dynamic memory allocation unlike the static approach in descriptor_variable
- Specifically designed for SQLDA (SQL Descriptor Area) functionality
- The returned variable structure must be managed by the caller
- Part of the ECPG preprocessor's support for dynamic SQL
- SQLDA provides a standard interface for working with result sets of unknown structure
- All type fields are initialized to safe default values (NULL or 0)
- Declared in preproc_extern.h for use throughout the ECPG preprocessor