# ind

## Location
[src/interfaces/ecpg/test/expected/preproc-array_of_struct.c:43-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-array_of_struct.c#L43-L48)

## Overview
The  symbol is a struct typedef that serves as an indicator structure for NULL value handling in ECPG (Embedded SQL in C for PostgreSQL) applications.

## Definition

```c
typedef  struct ind { 
#line 19 "array_of_struct.pgc"
 short name_ind ;
 
#line 20 "array_of_struct.pgc"
 short phone_ind ;
 } cust_ind ;
```
## Detailed Description
The  struct is used in ECPG test files as an indicator structure that accompanies host variables to detect NULL values in database operations. This is a common pattern in embedded SQL where indicator variables are used to handle NULL values returned from or passed to SQL operations. Each member corresponds to a specific field and indicates whether that field contains a NULL value.

## Parameters / Member Variables
- `name_ind`: Short integer indicator for the name field, used to detect NULL values
- `phone_ind`: Short integer indicator for the phone field, used to detect NULL values
## Dependencies
- Functions called/Symbols referenced: None (basic struct definition)
- Called from (representative examples):
  - Various ECPG test functions in array_of_struct, pointer_to_struct, and variable test files
  - Used extensively in ecpg_get_data function for NULL value handling
  - Referenced in bootstrap and analyze operations for index-related processing

## Notes and Other Information
- This is part of the ECPG test suite and demonstrates proper NULL handling patterns
- The indicator variables typically use negative values to indicate NULL, zero for non-NULL
- Found in expected test output files, indicating this is part of the PostgreSQL testing framework
- The symbol appears in multiple contexts: both as an ECPG indicator struct and as a variable name in various PostgreSQL core functions dealing with index operations