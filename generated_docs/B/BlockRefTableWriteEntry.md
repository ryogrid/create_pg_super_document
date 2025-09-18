# BlockRefTableWriteEntry

## Location
src/common/blkreftable.c: 817 - 854

## Overview
BlockRefTableWriteEntry appends a single BlockRefTableEntry to a block reference table file, converting the entry to serialized format and writing it through the writer's buffer.

## Definition
```c
void BlockRefTableWriteEntry(BlockRefTableWriter *writer, BlockRefTableEntry *entry)
```

## Detailed Description
This function serializes and writes a single BlockRefTableEntry to the output stream managed by the BlockRefTableWriter. The function converts the in-memory entry structure to a serialized format (BlockRefTableSerializedEntry), optimizes the data by trimming trailing zero entries from the chunk array, and then writes the serialized entry, chunk usage array, and actual chunk data in sequence.

The function is critical for incremental writing of block reference tables and requires entries to be provided in sorted order (by tablespace, database, relfilenumber, then fork number). The serialization process includes writing the entry metadata, followed by the non-zero chunk usage information, and finally the actual chunk data blocks.

## Parameters / Member Variables
- `writer`: The BlockRefTableWriter instance managing the output stream and buffer
- `entry`: The BlockRefTableEntry to be serialized and written to the output

## Dependencies
- Functions called/Symbols referenced:
  - BlockRefTableWrite (writes data through the buffer system)
  - BlockRefTableSerializedEntry (serialized format structure)
  - BlockRefTableWriter (writer structure type)
  - BlockRefTableEntry (input entry structure type)

- Called from (representative examples):
  - Functions that incrementally build block reference table files
  - Backup utilities that need to write sorted block reference entries
  - WAL processing code that generates block reference tables

## Notes and Other Information
- Entries MUST be written in sorted order (tablespace, database, relfilenumber, fork number)
- The function optimizes storage by trimming trailing zero chunks from the chunk usage array
- Three separate writes are performed: entry metadata, chunk usage array, and chunk data blocks
- Only non-zero chunks are written to minimize file size
- The caller is responsible for ensuring proper sort order - incorrect ordering will produce invalid output
- This is a void function that performs incremental writing as part of the larger block reference table generation process