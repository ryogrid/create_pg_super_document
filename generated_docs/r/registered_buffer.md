# registered_buffer

## Location
[src/backend/access/transam/xloginsert.c:87-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L87-L115)

## Overview
The  struct represents a registered buffer in PostgreSQL's WAL (Write-Ahead Log) insertion system, containing metadata and data for database pages that need to be included in WAL records.

## Definition

```c
structing a record.
 * 'hdr_scratch' is not a plain variable, but is palloc'd at initialization,
 * because we want it to be MAXALIGNed and padding bytes zeroed.
 *
 * For simplicity, it's allocated large enough to hold the headers for any
 * WAL record.
 */
static XLogRecData hdr_rdt;
```
## Detailed Description
The  structure is a core component of PostgreSQL's WAL insertion mechanism defined in . It serves as a container for buffer registration information when creating WAL records. Each instance tracks a specific database page that has been registered for inclusion in a WAL record through the  function. 

The structure manages both the metadata about the buffer (relation, block number, flags) and the actual data associated with it (page content, additional registered data). It supports various optimization strategies such as full-page images, compressed backup blocks, and differential logging through its flag system.

The struct is part of a static array  that maintains all currently registered buffers for the WAL record being constructed, allowing PostgreSQL to efficiently manage multiple page modifications within a single transaction.

## Parameters / Member Variables
- : Boolean flag indicating whether this slot in the registered_buffers array is currently active
- : Bitfield containing REGBUF_* flags that control buffer handling behavior (force image, no image, will init, standard layout, keep data, no change)
- : RelFileLocator structure that uniquely identifies the relation and provides access to the buffer's file location
- : Fork number (main, FSM, VM, etc.) specifying which fork of the relation this buffer belongs to
- : Block number within the fork identifying the specific page
- : Pointer to the actual page content in memory
- : Total length in bytes of all data registered with this buffer through XLogRegisterBufData calls
- : Head pointer of the linked list chain containing additional data associated with this buffer
- : Tail pointer of the rdata chain, or points to rdata_head if the chain is empty
- : Array of two temporary XLogRecData structures used during WAL record assembly to reference backup block data
- : Buffer space for storing compressed versions of full-page images when compression is enabled

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecData
  - [RelFileLocator](../R/RelFileLocator.md)
  - [ForkNumber](../F/ForkNumber.md)
  - BlockNumber
  - Page
  - COMPRESS_BUFSIZE

- Called from (representative examples):
  - [XLogEnsureRecordSpace](../X/XLogEnsureRecordSpace.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBlock](../X/XLogRegisterBlock.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogRecordAssemble](../X/XLogRecordAssemble.md)
  - [InitXLogInsert](../I/InitXLogInsert.md)

## Notes and Other Information
The  structure is central to PostgreSQL's crash recovery mechanism. The REGBUF_* flags provide fine-grained control over how buffers are handled in WAL records, supporting optimizations like avoiding full-page images for newly initialized pages (REGBUF_WILL_INIT) or forcing images for consistency (REGBUF_FORCE_IMAGE). The compressed_page buffer enables space-efficient storage of full-page images using various compression algorithms (PGLZ, LZ4, ZSTD). This structure is instantiated as part of a static array managed by the WAL insertion subsystem and is not directly accessible outside the xloginsert module.