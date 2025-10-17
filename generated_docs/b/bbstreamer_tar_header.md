# bbstreamer_tar_header

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:261-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L261-L318)

## Overview
Parses a tar file header block and extracts file metadata, determining whether the block represents a valid file header or marks the end of the archive.

## Definition
```c
static bool bbstreamer_tar_header(bbstreamer_tar_parser *mystreamer)
```

## Detailed Description
This function processes a complete tar header block (512 bytes) that has been buffered by the parser. It first checks if the block consists entirely of zero bytes, which indicates the end of the archive according to tar format specifications. If the block contains non-zero data, it parses the standard tar header fields including filename, file size, permissions, ownership, and file type. The function extracts this metadata into the bbstreamer_member structure and calculates the required padding bytes for proper tar block alignment. After successful parsing, it forwards the header block to the next bbstreamer in the chain with the appropriate context.

## Parameters / Member Variables
- `mystreamer`: Pointer to the tar parser containing the buffered header data and member information structure

## Dependencies
- Functions called/Symbols referenced:
  - [strlcpy](../s/strlcpy.md)
  - [read_tar_number](../r/read_tar_number.md)
  - [tarPaddingBytesRequired](../t/tarPaddingBytesRequired.md)
  - [bbstreamer_content](bbstreamer_content.md)
  - TAR_BLOCK_SIZE
  - TAR_OFFSET_NAME, TAR_OFFSET_SIZE, TAR_OFFSET_MODE, TAR_OFFSET_UID, TAR_OFFSET_GID
  - TAR_OFFSET_TYPEFLAG, TAR_OFFSET_LINKNAME
  - TAR_FILETYPE_DIRECTORY, TAR_FILETYPE_SYMLINK
  - BBSTREAMER_MEMBER_HEADER
- Called from (representative examples):
  - [bbstreamer_tar_parser_content](bbstreamer_tar_parser_content.md)

## Notes and Other Information
- Returns true if a valid file header was found and processed, false if end-of-archive detected
- Requires exactly TAR_BLOCK_SIZE (512) bytes to be buffered before invocation
- Validates that the filename field is not empty, failing with pg_fatal if it is
- Handles both regular files and special file types (directories and symbolic links)
- Calculates padding bytes needed to align file content to tar block boundaries
- End-of-archive detection is based on tar standard: a block of all zero bytes
- Forwards the complete header block to the next processing stage with BBSTREAMER_MEMBER_HEADER context

## Simplified Source

```c
static bool
bbstreamer_tar_header(bbstreamer_tar_parser *mystreamer)
{
    bool has_nonzero_byte = false;
    int i;
    bbstreamer_member *member = &mystreamer->member;
    char *buffer = mystreamer->base.bbs_buffer.data;

    Assert(mystreamer->base.bbs_buffer.len == TAR_BLOCK_SIZE);

    // Check for end-of-archive (all zero bytes)
    for (i = 0; i < TAR_BLOCK_SIZE; ++i) {
        if (buffer[i] != '\0') {
            has_nonzero_byte = true;
            break;
        }
    }

    // End of archive detected
    if (!has_nonzero_byte)
        return false;

    // Parse tar header fields
    strlcpy(member->pathname, &buffer[TAR_OFFSET_NAME], MAXPGPATH);
    if (member->pathname[0] == '\0')
        pg_fatal("tar member has empty name");

    member->size = read_tar_number(&buffer[TAR_OFFSET_SIZE], 12);
    member->mode = read_tar_number(&buffer[TAR_OFFSET_MODE], 8);
    member->uid = read_tar_number(&buffer[TAR_OFFSET_UID], 8);
    member->gid = read_tar_number(&buffer[TAR_OFFSET_GID], 8);

    // Determine file type
    member->is_directory = (buffer[TAR_OFFSET_TYPEFLAG] == TAR_FILETYPE_DIRECTORY);
    member->is_link = (buffer[TAR_OFFSET_TYPEFLAG] == TAR_FILETYPE_SYMLINK);
    if (member->is_link)
        strlcpy(member->linktarget, &buffer[TAR_OFFSET_LINKNAME], 100);

    // Calculate padding bytes for block alignment
    mystreamer->pad_bytes_expected = tarPaddingBytesRequired(member->size);

    // Forward header to next bbstreamer
    bbstreamer_content(mystreamer->base.bbs_next, member,
                      buffer, TAR_BLOCK_SIZE,
                      BBSTREAMER_MEMBER_HEADER);

    return true;
}
```