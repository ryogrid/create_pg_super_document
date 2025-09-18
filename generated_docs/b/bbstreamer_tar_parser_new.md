# bbstreamer_tar_parser_new

## Location
src/bin/pg_basebackup/bbstreamer_tar.c: 93 - 110

## Overview
Creates a new bbstreamer that parses a stream of content as tar data, converting untyped chunks into typed chunks according to tar format conventions.

## Definition


## Detailed Description
This function creates and initializes a bbstreamer_tar_parser instance that serves as a tar format parser in the backup streaming pipeline. The parser takes input as a series of BBSTREAMER_UNKNOWN chunks and processes them to produce typed chunks (such as BBSTREAMER_MEMBER_HEADER, BBSTREAMER_MEMBER_CONTENTS, etc.) that downstream components can understand. The parser maintains state to track its position within tar file structure and handles the transition between different tar member components.

## Parameters / Member Variables
- : The next bbstreamer in the processing chain that will receive the parsed, typed chunks

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - initStringInfo
  - bbstreamer_tar_parser_ops
  - BBSTREAMER_MEMBER_HEADER
- Called from (representative examples):
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md)
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md)

## Notes and Other Information
- The function allocates memory for the parser structure using palloc0, ensuring zero-initialization
- Sets up the operations table (bbstreamer_tar_parser_ops) which defines the parser's behavior
- Initializes the internal buffer using StringInfo for handling partial data
- Sets the initial parsing context to BBSTREAMER_MEMBER_HEADER, indicating it expects to parse a tar header first
- Returns the base bbstreamer pointer, allowing it to be used polymorphically in the streaming chain