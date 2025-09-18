# XLogRecordBlockHeader

## Location
src/include/access/xlogrecord.h: 103 - 113

## Overview
XLogRecordBlockHeader is a structure that provides header information for block data appended to an XLOG record, containing metadata about specific data blocks that are part of a WAL record.

## Definition


## Detailed Description
XLogRecordBlockHeader serves as a descriptor for block-specific data within WAL records. Each header describes one data block that is part of the logged operation, providing essential information about the block's identity, type, and data length. The structure is designed to be compact and is followed by variable-length components that depend on the flag bits set in fork_flags. The structure is intentionally not aligned, requiring copying to aligned local storage before use for performance reasons.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Block reference ID that uniquely identifies this block within the WAL record context
- : Combined field containing both the relation fork identifier and various flag bits that control record format
- : Length in bytes of the resource manager-specific payload data associated with this block (excludes any full page image and the header itself)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)

- Called from (representative examples):
  - [XLogRecordAssemble](XLogRecordAssemble.md)
  - SizeOfXLogRecordBlockHeader

## Notes and Other Information
- The structure is intentionally not aligned for space efficiency, requiring copying to aligned storage before use
- [Variable](../V/Variable.md)-length data follows this header based on flag bits: XLogRecordBlockImageHeader, RelFileLocator, and BlockNumber
- The data_length field specifically excludes the size of any full page image that may be included
- This header is part of the variable-length portion of WAL records that follows the fixed XLogRecord header
- Multiple XLogRecordBlockHeader structures can appear in a single WAL record when multiple blocks are affected