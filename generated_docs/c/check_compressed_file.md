# check_compressed_file

## Location
src/bin/pg_dump/compress_io.c: 220 - 240

## Overview
Checks if a compressed file with a specified extension exists at the given path and updates the filename buffer with the constructed full filename.

## Definition
static bool check_compressed_file(const char *path, char **fname, char *ext)

## Detailed Description
This is a static utility function used internally within the compression I/O module to detect the presence of compressed files. It constructs a filename by appending the provided extension to the base path, checks if this file exists using the access() system call, and updates the filename buffer with the constructed name. The function properly manages memory by freeing any existing buffer contents before allocating a new one.

## Parameters / Member Variables
- path: The base file path without extension
- fname: A pointer to a character pointer that will be updated with the constructed filename (existing buffer is freed and replaced)
- ext: The file extension to append to the path

## Dependencies
- Functions called/Symbols referenced:
  - free_keep_errno
  - psprintf
  - access
  - F_OK
- Called from (representative examples):
  - InitDiscoverCompressFileHandle (multiple calls for different compression formats)

## Notes and Other Information
This function is marked as static, making it internal to the compress_io.c module. It follows a pattern of destructively updating the fname parameter, which requires careful memory management by callers. The function uses F_OK with access() to check for file existence without requiring read permissions. The free_keep_errno function is used to preserve errno values during memory deallocation.