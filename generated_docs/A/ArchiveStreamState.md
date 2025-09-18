# ArchiveStreamState

## Location
src/bin/pg_basebackup/pg_basebackup.c: 56 - 65

## Overview
A state management structure for handling archive stream processing during base backup operations, including compression, streaming, and manifest generation.

## Definition


## Detailed Description
ArchiveStreamState is a comprehensive state management structure used in pg_basebackup to coordinate the processing of archive streams during base backup operations. This structure encapsulates all the necessary components for handling data streaming, compression, and manifest generation in a unified manner.

The structure manages the complex pipeline of data processing that occurs during backup operations, including the coordination of multiple streaming components, compression settings, and manifest file generation. It serves as a central state holder that allows the backup process to maintain consistency across different phases of archive stream processing.

## Parameters / Member Variables
- : Identifier for the tablespace being processed, used to track which tablespace the current stream represents
- : Pointer to compression specification structure that defines the compression method and parameters to be applied
- : Primary bbstreamer instance responsible for handling the main data stream processing
- : Secondary bbstreamer used specifically for injecting manifest information into the stream
- : PQExpBuffer used for buffering manifest data during processing
- : File path for the manifest file (maximum MAXPGPATH characters)
- : File handle for the open manifest file being written to

## Dependencies
- Functions called/Symbols referenced:
  - [pg_compress_specification](../p/pg_compress_specification.md) (compression configuration)
  - [bbstreamer](../b/bbstreamer.md) (stream processing components)
  - [manifest_file](../m/manifest_file.md) (FILE structure for manifest output)
- Called from (representative examples):
  - [ReceiveArchiveStream](../R/ReceiveArchiveStream.md)
  - [ReceiveArchiveStreamChunk](../R/ReceiveArchiveStreamChunk.md)

## Notes and Other Information
- This structure coordinates multiple aspects of backup stream processing in a single state object
- The dual streamer design (streamer and manifest_inject_streamer) allows for parallel processing of data and manifest information
- The manifest components (manifest_buffer, manifest_filename, manifest_file) work together to generate backup manifests
- Used specifically in the context of receiving and processing archive streams during pg_basebackup operations
- The structure enables stateful processing across multiple function calls during stream handling
- Compression and streaming are decoupled through the compress and streamer components, allowing for flexible configuration