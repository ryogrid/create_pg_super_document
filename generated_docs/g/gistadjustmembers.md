# gistadjustmembers

## Location
[src/backend/access/gist/gistvalidate.c:290-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistvalidate.c#L290-L354)

## Overview
Preconfigures dependency relationships for operators and support functions when adding them to a GiST operator family, establishing appropriate hard and soft dependencies based on their roles.

## Definition

```c
void
gistadjustmembers(Oid opfamilyoid,
				  Oid opclassoid,
				  List *operators,
				  List *functions)
```
## Detailed Description
The  function is a prechecking function called during the process of adding operators and functions to a GiST operator family. It configures the dependency relationships that determine how these database objects depend on each other for purposes of DROP CASCADE operations and other dependency management.

The function implements a specific dependency strategy for GiST access methods:

**For Operators**: All operators are assigned soft dependencies pointing to the operator family rather than hard dependencies. This design recognizes that operator membership in a GiST opfamily is determined by the support functions' logic and can be altered without breaking the operators themselves.

**For Functions**: The dependency type varies based on whether the function is required or optional:
- **Required functions** (CONSISTENT, UNION, PENALTY, PICKSPLIT, EQUAL) get hard dependencies, making them essential to the operator class
- **Optional functions** (COMPRESS, DECOMPRESS, DISTANCE, FETCH, OPTIONS, SORTSUPPORT) get soft family dependencies, allowing more flexible management

This dependency configuration ensures proper cascading behavior when operator classes or families are dropped while maintaining the flexibility needed for GiST's extensible architecture.

## Parameters / Member Variables
- `opfamilyoid`: The OID of the operator family to which members are being added
- `opclassoid`: The OID of the operator class (may be used for dependency targeting)
- `*operators`: List of  structures representing operators to be added
- `*functions`: List of  structures representing support functions to be added
## Dependencies
- Functions called/Symbols referenced:
  -  - Structure representing operator family members
  - , , , ,  - Required function type constants
  - , , , , ,  - Optional function type constants
  -  - Error reporting for invalid function numbers
- Called from:
  -  at src/backend/access/gist/gist.c:98

## Notes and Other Information
- This function modifies the , , and  fields of  structures
- The dependency strategy reflects GiST's flexible architecture where operator membership depends on support function logic
- Hard dependencies ensure that required functions cannot be dropped while the operator class exists
- Soft dependencies allow optional functions to be dropped independently
- The function throws an ERROR for unrecognized support function numbers, ensuring only valid GiST function types are processed
- Generally called during CREATE OPERATOR CLASS or ALTER OPERATOR FAMILY operations
- The design assumes that GiST operator classes rarely share operator families, simplifying dependency management