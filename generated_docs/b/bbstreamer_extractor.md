# bbstreamer_extractor

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:29-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L29-L37)

## Overview
A specialized bbstreamer structure designed to extract and write files from backup archives, providing archive extraction functionality during PostgreSQL base backup restore operations.

## Definition
```c
typedef struct bbstreamer_extractor
{
    bbstreamer  base;
    char       *basepath;
    const char *(*link_map) (const char *);
    void        (*report_output_file) (const char *);
    char        filename[MAXPGPATH];
    FILE       *file;
} bbstreamer_extractor;
```

## Detailed Description
The `bbstreamer_extractor` is a concrete implementation of the bbstreamer interface specifically designed for extracting files from backup archives during restore operations. This structure extends the base bbstreamer functionality to handle the complex process of reading archive metadata, creating directory structures, and writing individual files to the filesystem. It operates on typed chunks that contain file metadata and content, allowing it to reconstruct the original file structure from a backup archive.

The extractor supports symbolic link remapping through a callback function and can report each file extraction through another callback, providing flexibility for different restoration scenarios and progress reporting.

## Parameters / Member Variables
- `base`: The base bbstreamer structure containing common streaming functionality and operation callbacks
- `basepath`: Base directory path where all extracted files will be written, serving as the root for relative paths in the archive
- `link_map`: Function pointer for remapping symbolic link targets, allowing path translation during extraction (can be NULL)
- `report_output_file`: Callback function called when each new output file is opened, used for progress reporting (can be NULL)
- `filename`: Buffer to store the current file path being extracted (sized to MAXPGPATH)
- `file`: FILE pointer to the currently open output file being written

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base structure)
  - MAXPGPATH (path length constant)
- Called from (representative examples):
  - [bbstreamer_extractor_new](bbstreamer_extractor_new.md)
  - [bbstreamer_extractor_content](bbstreamer_extractor_content.md)
  - [bbstreamer_extractor_finalize](bbstreamer_extractor_finalize.md)
  - [bbstreamer_extractor_free](bbstreamer_extractor_free.md)

## Notes and Other Information
- Requires typed chunks following the rules described in bbstreamer.h, cannot process raw untyped data
- [Archive](../A/Archive.md) format is abstracted away - works with any archive format as long as proper member information is provided
- The link_map function allows for path remapping of symbolic links during extraction, useful for relocating restored databases
- Progress reporting through report_output_file callback enables user feedback during long restore operations
- Automatically handles directory creation and file permissions during extraction
- Located in src/bin/pg_basebackup/bbstreamer_file.c:29-37