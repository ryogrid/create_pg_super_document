# spgConfigIn

## Location
[src/include/access/spgist.h:36-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist.h#L36-L39)

## Overview
A struct that serves as input parameter for the SP-GiST opclass config method, containing information about the data type to be indexed.

## Definition


## Detailed Description
spgConfigIn is a simple input structure used in the SP-GiST (Space-Partitioned Generalized Search Tree) index access method. It is passed to the opclass config method to provide information about the data type that will be indexed. The config method uses this information to determine appropriate configuration parameters for the index, which are returned in the corresponding spgConfigOut structure.

## Parameters / Member Variables
- : OID of the data type to be indexed. This identifies the PostgreSQL data type for which the SP-GiST index is being configured.

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [spgGetCache](spgGetCache.md) (src/backend/access/spgist/spgutils.c:189)
  - [spgvalidate](spgvalidate.md) (src/backend/access/spgist/spgvalidate.c:57)

## Notes and Other Information
- This struct is part of the SP-GiST index access method interface
- It works in conjunction with spgConfigOut to allow opclass config methods to receive input parameters and return configuration settings
- The structure is intentionally simple, containing only the essential information needed to configure an SP-GiST index for a specific data type