# OpFamilyMember

## Location
src/include/access/amapi.h: 84 - 95

## Overview
OpFamilyMember is a struct used to track both operators and support functions while building or adding to an operator class (opclass) or operator family (opfamily) in PostgreSQL's access method framework.

## Definition


## Detailed Description
The OpFamilyMember structure serves as a unified representation for both operators and support functions during the construction and modification of operator classes and families. It encapsulates all necessary metadata including object identification, dependency relationships, and type information.

The structure supports PostgreSQL's dependency management system by distinguishing between "hard" and "soft" dependencies. Hard dependencies (ref_is_hard=true) create NORMAL dependencies on the operator/function and INTERNAL dependencies on the opclass/opfamily, preventing drops without CASCADE and disallowing ALTER OPERATOR FAMILY DROP. Soft dependencies (ref_is_hard=false) create AUTO dependencies that allow automatic cleanup when referenced objects are dropped.

The amadjustmembers functions receive lists of these structures and can modify their "ref" fields to adjust dependency behavior based on access method requirements.

## Parameters / Member Variables
- : Boolean flag indicating whether this member represents a support function (true) or an operator (false)
- : OID of the operator or support function being referenced
- : Strategy number for operators or support function number for functions
- : OID of the left operand data type for the operator/function
- : OID of the right operand data type for the operator/function
- : OID of the sort operator family for ordering operators, or 0 if not applicable
- : Boolean determining dependency strength - true for hard (NORMAL/INTERNAL), false for soft (AUTO)
- : Boolean indicating whether the dependency is on an opfamily (true) or opclass (false)
- : OID of the operator class or operator family that this member belongs to

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure definition)
- Called from (representative examples):
  - [ginadjustmembers](../g/ginadjustmembers.md) (GIN access method)
  - [gistadjustmembers](../g/gistadjustmembers.md) (GiST access method)
  - [btadjustmembers](../b/btadjustmembers.md) (B-tree access method)
  - [hashadjustmembers](../h/hashadjustmembers.md) (Hash access method)
  - [spgadjustmembers](../s/spgadjustmembers.md) (SP-GiST access method)
  - [DefineOpClass](../D/DefineOpClass.md) (operator class definition)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md) (adding members to operator families)
  - [AlterOpFamilyDrop](../A/AlterOpFamilyDrop.md) (removing members from operator families)

## Notes and Other Information
- This structure is central to PostgreSQL's extensible operator class system, allowing custom access methods to define their own operators and support functions
- The dependency management features help maintain referential integrity in the system catalogs
- Different access methods (B-tree, Hash, GiST, SP-GiST, GIN) all use this common structure but may interpret certain fields differently
- The sortfamily field is specifically used for ordering operators in access methods that support ordered scans
- Lists of OpFamilyMember structures are commonly passed to access method adjustment functions during DDL operations