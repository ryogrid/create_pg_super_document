# bytea

## Location
src/include/c.h: 699 - 699

## Overview
The  type is PostgreSQL's binary data type, implemented as a typedef of the  structure, designed to store arbitrary binary data without interpretation or null-termination.

## Definition


## Detailed Description
The  type is one of PostgreSQL's core binary data types, built directly on top of the  structure. It provides a way to store arbitrary sequences of bytes without any interpretation, encoding, or null-termination requirements. Unlike text types,  can safely store binary data including null bytes, making it suitable for storing images, documents, encrypted data, and other binary content.

Key characteristics:
- **Pure Binary Storage**: Can store any sequence of bytes including null bytes (\x00)
- **No Character Encoding**: Data is stored exactly as provided without character set conversion
- **Variable Length**: Can store from 0 bytes up to approximately 1GB (limited by TOAST)
- **TOAST Support**: Large bytea values are automatically compressed and/or stored out-of-line
- **Hex and Escape Output**: Supports both hexadecimal and escape sequence output formats

The data length is determined by  and there is no terminating null byte.

## Parameters / Member Variables
As a typedef of ,  inherits:
- : Length and metadata field (do not access directly)
- : Raw binary data content

## Dependencies
- Functions called/Symbols referenced:
  - varlena (base structure)
- Called from (representative examples):
  - bytea_catenate
  - bytea_substring
  - bytea_overlay
  - Functions in varlena.c
  - Various I/O and manipulation functions

## Notes and Other Information
- **Binary Safe**: Unlike text types, can safely store any binary data including control characters
- **Output Formats**: PostgreSQL supports both hex format (\x...) and escape format for bytea output
- **Performance**: Efficient for binary data storage with automatic compression for large values
- **Use Cases**: Commonly used for storing images, documents, cryptographic data, and serialized objects
- **Size Limits**: Practical size limit is ~1GB due to TOAST storage limitations
- **Comparison**: Bytea values are compared byte-by-byte in binary order
- **Indexing**: Can be indexed using btree, hash, and GIN indexes depending on use case