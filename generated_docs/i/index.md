# index

## Location
[src/interfaces/ecpg/preproc/type.h:94-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L94-L100)

## Overview
The 'index' struct is used in the ECPG parser to track array dimension and string length information during preprocessing of C variable declarations with embedded SQL.

## Definition

```c
struct index
{
	char	   *index1;
	char	   *index2;
	char	   *str;
};
```
## Detailed Description
The 'index' struct is a parser utility structure used in the ECPG (Embedded SQL in C) preprocessor to handle array dimensions and string lengths during variable declaration processing. It is primarily used in the YACC/Bison grammar rules for parsing variable declarations that may include array bounds or string size specifications.

The structure supports two-dimensional indexing where index1 typically represents the primary dimension (array size) and index2 represents a secondary dimension or string length. The parser uses special values like "-1" to indicate unspecified or default dimensions, which are later converted to appropriate values ("0" for empty arrays) during processing.

This struct is part of the parser's union type system and is used during the semantic analysis phase to collect dimension information before creating the final ECPGtype structures that represent the complete type information.

## Parameters / Member Variables
- : String representing the first dimension, typically array size or primary dimension (uses "-1" for unspecified)
- : String representing the second dimension, typically string length or secondary dimension (uses "-1" for unspecified)  
- : String representation of the dimension syntax as it appears in the source code (e.g., "[10][20]")

## Dependencies
- Functions called/Symbols referenced:
  - mm_strdup (for string duplication during parsing)
  - Used in parser grammar rules for array bounds processing
- Called from (representative examples):
  - opt_array_bounds grammar rules in ecpg.addons
  - Variable declaration processing in ecpg.trailer
  - add_typedef function calls for type definition creation

## Notes and Other Information
- This structure is part of the ECPG preprocessor's YACC/Bison parser union type
- Used during parsing phase before final ECPGtype structures are created
- The string "-1" is used as a sentinel value to indicate unspecified dimensions
- Conversion logic handles transformation of "-1" values to "0" for arrays or actual size values
- The str field maintains the original syntax representation for code generation purposes
- Memory for string fields is managed using mm_strdup() and the ECPG memory management system
- Primarily used in opt_array_bounds parsing rules to handle C array and string declarations in embedded SQL contexts