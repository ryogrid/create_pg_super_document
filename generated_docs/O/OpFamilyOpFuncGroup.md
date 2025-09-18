# OpFamilyOpFuncGroup

## Location
src/include/access/amvalidate.h: 20 - 26

## Overview
OpFamilyOpFuncGroup is a structure used to group operators and support functions by their left and right data types within PostgreSQL operator families, facilitating operator family validation.

## Definition


## Detailed Description
OpFamilyOpFuncGroup is a data structure returned by the identify_opfamily_groups() function to represent a group of operators and support functions that share the same left and right data types within an operator family. This struct is essential for PostgreSQL's access method validation system, which ensures that operator families contain consistent and complete sets of operators and support functions.

The structure uses bitmasks to efficiently track which operators and functions are present for each data type combination. For example, if strategy K is present for a specific lefttype/righttype combination, bit (1 << K) is set in the operatorset field. This allows validation routines to quickly check for missing or inconsistent operator implementations across different access methods like B-tree, hash, GIN, GiST, SP-GiST, and BRIN indexes.

With uint64 fields, the structure can handle operator and function numbers up to 63, which provides ample capacity for current and future PostgreSQL access method requirements.

## Parameters / Member Variables
- : OID of the left operand data type (corresponds to amoplefttype/amproclefttype in catalog tables)
- : OID of the right operand data type (corresponds to amoprighttype/amprocrighttype in catalog tables)
- : Bitmask where bit K is set if operator strategy K exists for this lefttype/righttype combination
- : Bitmask where bit K is set if support function K exists for this lefttype/righttype combination

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - uint64 (unsigned 64-bit integer type)
- Called from (representative examples):
  - [brinvalidate](../b/brinvalidate.md) (src/backend/access/brin/brin_validate.c:53, 210)
  - [ginvalidate](../g/ginvalidate.md) (src/backend/access/gin/ginvalidate.c:46, 218)  
  - [gistvalidate](../g/gistvalidate.md) (src/backend/access/gist/gistvalidate.c:48, 244)
  - [hashvalidate](../h/hashvalidate.md) (src/backend/access/hash/hashvalidate.c:61, 210)
  - [btvalidate](../b/btvalidate.md) (src/backend/access/nbtree/nbtvalidate.c:55, 192)
  - [spgvalidate](../s/spgvalidate.md) (src/backend/access/spgist/spgvalidate.c:54, 152, 264)
  - [identify_opfamily_groups](../i/identify_opfamily_groups.md) (src/backend/access/index/amvalidate.c:46, 121)

## Notes and Other Information
- This structure is primarily used in operator family validation routines across all PostgreSQL access methods
- The bitmask approach allows for efficient checking of operator and function completeness
- The 64-bit limitation for strategy/function numbers is not a practical constraint given current PostgreSQL requirements
- Each OpFamilyOpFuncGroup instance represents one unique combination of lefttype and righttype within an operator family
- The structure is allocated using palloc() and typically stored in lists for processing by validation functions