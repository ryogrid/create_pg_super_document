# spgConfigOut

## Location
[src/include/access/spgist.h:41-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist.h#L41-L48)

## Overview
A struct that serves as output parameter for the SP-GiST opclass config method, containing configuration information determined by the opclass for index operations.

## Definition

```c
typedef struct spgConfigOut
{
	Oid			prefixType;		/* Data type of inner-tuple prefixes */
	Oid			labelType;		/* Data type of inner-tuple node labels */
	Oid			leafType;		/* Data type of leaf-tuple values */
	bool		canReturnData;	/* Opclass can reconstruct original data */
	bool		longValuesOK;	/* Opclass can cope with values > 1 page */
} spgConfigOut;
```
## Detailed Description
spgConfigOut is an output structure used in the SP-GiST (Space-Partitioned Generalized Search Tree) index access method. It is filled by the opclass config method to specify how the index should be configured for a particular data type. This structure defines the data types used for different parts of the index structure and capabilities of the opclass.

## Parameters / Member Variables
- : OID of the data type used for inner-tuple prefixes. Inner tuples can store prefix information to optimize searches
- : OID of the data type used for inner-tuple node labels. These labels identify branches in the tree structure
- : OID of the data type used for leaf-tuple values. This is typically the same as the indexed data type but can differ
- : Boolean indicating whether the opclass can reconstruct the original indexed data from the index. This affects index-only scan capabilities
- : Boolean indicating whether the opclass can handle values longer than one page. This determines storage and retrieval strategies

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - [bool](../b/bool.md) (PostgreSQL boolean type)
- Called from (representative examples):
  - [spg_kd_config](spg_kd_config.md) (src/backend/access/spgist/spgkdtreeproc.c:31)
  - [spg_quad_config](spg_quad_config.md) (src/backend/access/spgist/spgquadtreeproc.c:30)
  - [spg_text_config](spg_text_config.md) (src/backend/access/spgist/spgtextproc.c:99)
  - [spgvalidate](spgvalidate.md) (src/backend/access/spgist/spgvalidate.c:58)
  - [spg_box_quad_config](spg_box_quad_config.md) (src/backend/utils/adt/geo_spgist.c:403)
  - [inet_spg_config](../i/inet_spg_config.md) (src/backend/utils/adt/network_spgist.c:54)
  - [spg_range_quad_config](spg_range_quad_config.md) (src/backend/utils/adt/rangetypes_spgist.c:63)

## Notes and Other Information
- This struct is part of the SP-GiST index access method interface
- It works in conjunction with spgConfigIn to allow opclass config methods to receive input parameters and return configuration settings
- The configuration affects how the SP-GiST index stores and retrieves data
- Different opclasses (k-d tree, quad tree, text, etc.) fill this structure differently based on their specific requirements
- The structure is used by both built-in and user-defined SP-GiST opclasses