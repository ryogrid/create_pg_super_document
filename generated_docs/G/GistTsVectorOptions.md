# GistTsVectorOptions

## Location
[src/backend/utils/adt/tsgistidx.c:33-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L33-L34)

## Overview
A structure that defines opclass options for the tsvector_ops GiST index opclass, specifically controlling the signature length used in the indexing operations.

## Definition

```c
typedef char *BITVECP;
```
## Detailed Description
 is a configuration structure used to customize the behavior of GiST (Generalized Search Tree) indexes on tsvector data types. The primary purpose is to allow users to specify the signature length for the GiST index operations, which affects both storage requirements and search performance. The structure follows PostgreSQL's varlena format, making it suitable for storage as a variable-length data type.

The signature length determines how many bits are used in the signature-based filtering during index operations. A longer signature provides better filtering (fewer false positives) but requires more storage space, while a shorter signature uses less space but may have more false positives during searches.

## Parameters / Member Variables
- : Standard PostgreSQL varlena header containing the total size of the structure. This should not be manipulated directly by user code.
- : The signature length in bytes used for GiST index operations. This value controls the precision of the signature-based filtering mechanism.

## Dependencies
- Functions called/Symbols referenced:
  - Uses standard PostgreSQL varlena format
- Called from (representative examples):
  - : Macro that extracts the siglen value from opclass options
  - : Function that handles opclass option processing

## Notes and Other Information
- Default signature length is defined as  (31 * 4 = 124 bytes)
- Maximum signature length is limited by  (GISTMaxIndexKeySize)
- The  macro provides a convenient way to access the signature length, falling back to the default value when no options are specified
- This structure is part of PostgreSQL's full-text search indexing infrastructure and is specifically designed for optimizing tsvector GiST index performance