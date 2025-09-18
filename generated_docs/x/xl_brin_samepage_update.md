# xl_brin_samepage_update

## Location
src/include/access/brin_xlog.h: 102 - 105

## Overview
A minimal WAL record structure for logging BRIN tuple updates that occur within the same page, requiring only the offset of the tuple being updated.

## Definition


## Detailed Description
The  structure is the simplest of the BRIN WAL record types, designed for updates where the modified BRIN tuple can fit in the same location on the same page. This is the most efficient type of BRIN update since it doesn't require revmap changes or cross-page operations.

Unlike cross-page updates that require multiple backup blocks, samepage updates only need backup block 0, which contains the updated page with the new BrinTuple data. The revmap doesn't need to be updated because the tuple remains in the same location, and no old page backup is needed since the operation happens in-place.

## Parameters / Member Variables
- : The offset number within the page where the tuple being updated is located

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (type)
- Called from (representative examples):
  - [brin_doupdate](../b/brin_doupdate.md) (in src/backend/access/brin/brin_pageops.c:186)
  - [brin_xlog_samepage_update](../b/brin_xlog_samepage_update.md) (in src/backend/access/brin/brin_xlog.c:173, 177)
  - [brin_desc](../b/brin_desc.md) (in src/backend/access/rmgrdesc/brindesc.c:54)

## Notes and Other Information
- This is the most efficient BRIN update operation as it requires only one backup block
- No revmap update is needed since the tuple location doesn't change
- The simplicity of this structure reflects the efficiency of in-place updates in BRIN indexes
- This structure contrasts with  which handles more complex cross-page updates
- The new tuple data is stored directly in the backup block rather than as part of the WAL record structure
- This operation is preferred when the updated tuple can fit in its original location without exceeding page space constraints