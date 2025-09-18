# bbstreamer_tar_archiver

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:39-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L39-L43)

## Overview
A structure representing a TAR format archiver that implements the bbstreamer interface to generate and modify TAR archive streams in PostgreSQL's pg_basebackup utility.

## Definition


## Detailed Description
The `bbstreamer_tar_archiver` structure is designed to generate or modify TAR archives in pg_basebackup. It extends the base `bbstreamer` structure to provide TAR-specific archiving functionality. This archiver processes typed chunks (header, content, trailer) and creates valid TAR archive output. It can construct new TAR headers from metadata, ensure proper padding, and generate correct archive trailers. The archiver is intended to be used either for generating brand-new tar archives or for modifying existing ones on the fly.

## Parameters / Member Variables
- `base`: Base bbstreamer structure containing common streamer functionality (operations, next streamer, buffer)  
- `rearchive_member`: Boolean flag indicating whether the current archive member needs to be re-archived with newly constructed headers and padding

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base structure)
  - tarCreateHeader (for generating TAR headers)
  - [tarPaddingBytesRequired](../t/tarPaddingBytesRequired.md) (for calculating padding bytes)
  - [bbstreamer_content](bbstreamer_content.md) (for forwarding processed data)
- Called from (representative examples):
  - [bbstreamer_tar_archiver_new](bbstreamer_tar_archiver_new.md) (constructor function)
  - [bbstreamer_tar_archiver_content](bbstreamer_tar_archiver_content.md) (content processing function)

## Notes and Other Information
- This structure is defined in src/bin/pg_basebackup/bbstreamer_tar.c:39-43
- The archiver validates and fixes TAR format compliance by replacing zero-length headers with properly constructed ones
- It handles TAR block alignment by generating appropriate padding bytes  
- [Archive](../A/Archive.md) trailers are standardized to two blocks of zero bytes for compatibility
- The `rearchive_member` flag ensures consistent handling when headers are regenerated and corresponding trailers need adjustment
- Designed to work in streaming fashion, processing data incrementally without requiring the entire archive in memory