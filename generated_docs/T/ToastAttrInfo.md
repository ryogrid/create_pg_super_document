# ToastAttrInfo

## Location
[src/include/access/toast_helper.h:36-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/toast_helper.h#L36-L62)

## Overview
ToastAttrInfo is a structure that stores information about one column of a tuple being processed by PostgreSQL TOAST (The Oversized-Attribute Storage Technique) mechanism, containing metadata needed for compression and out-of-line storage operations.

## Definition
```c
typedef struct
{
    struct varlena *tai_oldexternal;
    int32           tai_size;
    uint8           tai_colflags;
    char            tai_compression;
} ToastAttrInfo;
```

## Detailed Description
ToastAttrInfo serves as a per-column information holder during TOAST operations, which are PostgreSQL mechanism for handling large variable-length attributes. This structure is part of the TOAST helper framework that allows table access methods to implement compressed or out-of-line storage of varlena attributes.

The structure is used in conjunction with ToastTupleContext to provide detailed tracking of individual column states during TOAST processing. It stores critical information about the column current state, previous external values, size calculations, compression settings, and processing flags that guide the TOAST algorithm decisions.

Each ToastAttrInfo instance corresponds to one column in a tuple being processed, and arrays of these structures are maintained within ToastTupleContext to handle multi-column TOAST operations efficiently.

## Parameters / Member Variables
- `tai_oldexternal`: Pointer to the previous external (out-of-line) varlena value for this column, used when updating existing tuples to manage cleanup of old TOAST values
- `tai_size`: Size in bytes of the current column value, calculated and used for TOAST decision-making algorithms to determine compression and externalization priorities
- `tai_colflags`: Bitfield containing column-specific TOAST processing flags such as TOASTCOL_NEEDS_DELETE_OLD, TOASTCOL_NEEDS_FREE, TOASTCOL_IGNORE, and TOASTCOL_INCOMPRESSIBLE
- `tai_compression`: Character indicating the compression method or storage strategy, with values like space for default handling, TYPSTORAGE_PLAIN for already processed columns, or TYPSTORAGE_EXTENDED for incompressible but externalizable data

## Dependencies
- Functions called/Symbols referenced:
  - struct varlena (from c.h)
  - int32, uint8, char (basic types)
- Used by (representative examples):
  - ToastTupleContext.ttc_attr
  - toast_tuple_try_compression
  - toast_tuple_externalize
  - toast_tuple_cleanup
  - heap_toast_insert_or_update

## Notes and Other Information
- The tai_size field is only made valid for varlena attributes with toast_action[i] different from TYPSTORAGE_PLAIN
- Column flags follow the same bit pattern as the overall TOAST tuple flags but apply specifically to individual columns
- The structure is designed to work as part of an array with length equal to ttc_rel->rd_att->natts (number of attributes in the relation)
- Contents of ToastAttrInfo arrays do not need to be initialized before calling toast_tuple_init(), as the initialization is handled by the TOAST framework
- The tai_compression field uses the same constants as the PostgreSQL type storage strategy system (TYPSTORAGE_PLAIN, TYPSTORAGE_EXTENDED, etc.)