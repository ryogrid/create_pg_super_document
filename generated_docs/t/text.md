# text

## Location
src/include/c.h: 700 - 700

## Overview
The  type is PostgreSQL's primary variable-length character string type, implemented as a typedef of the  structure, designed to store UTF-8 encoded text data without null-termination.

## Definition


## Detailed Description
The  type is PostgreSQL's fundamental character string data type, built on the  structure. It stores variable-length character strings in UTF-8 encoding without requiring null-termination. This design makes it highly efficient for string operations while supporting full Unicode character sets.

Key characteristics:
- **UTF-8 Encoding**: All text data is stored in UTF-8 encoding by default
- **Variable Length**: Can store strings from 0 characters up to approximately 1GB
- **No Null Termination**: Length is determined by the varlena header, not null bytes
- **Unicode Support**: Full support for Unicode characters and collations
- **TOAST Integration**: Large text values are automatically compressed and/or stored out-of-line
- **Collation Aware**: Supports various collation rules for sorting and comparison

The actual string data length is determined by  and the text content can contain embedded null bytes if needed.

## Parameters / Member Variables
As a typedef of ,  inherits:
- : Length and metadata field (do not access directly)
- : UTF-8 encoded text content

## Dependencies
- Functions called/Symbols referenced:
  - varlena (base structure)
- Called from (representative examples):
  - text_catenate
  - text_substring
  - text_overlay
  - text_position
  - text_cmp
  - split_text
  - Various text manipulation and I/O functions

## Notes and Other Information
- **Character Encoding**: Always uses UTF-8 encoding in modern PostgreSQL installations
- **Performance**: Highly optimized for string operations with efficient memory layout
- **Comparison**: Text comparison respects collation rules and can be case-sensitive or case-insensitive
- **Indexing**: Supports various index types including B-tree, Hash, GIN, and GiST for full-text search
- **Full-Text Search**: Integrates with PostgreSQL's full-text search capabilities
- **Regular Expressions**: Supports POSIX regular expressions and pattern matching
- **Internationalization**: Full support for international character sets and locale-specific operations
- **Memory Efficiency**: Uses TOAST for large strings, keeping frequently accessed data inline
- **SQL Standard**: Closely follows SQL standard behavior for character string types