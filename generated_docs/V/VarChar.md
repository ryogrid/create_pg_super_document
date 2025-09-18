# VarChar

## Location
src/include/c.h: 702 - 722

## Overview
The  type implements PostgreSQL's SQL  data type, providing variable-length character strings with an optional maximum length constraint.

## Definition


## Detailed Description
The  type is PostgreSQL's implementation of the SQL standard  data type. It stores variable-length character strings with an optional maximum length specification. Unlike , VARCHAR does not pad strings with spaces and stores only the actual characters provided, making it more space-efficient for varying string lengths.

Key characteristics:
- **Variable Length**: Stores strings of any length up to the specified maximum (or unlimited if no max specified)
- **No Padding**: Does not add trailing spaces like CHAR(n) types
- **Length Constraint**: Can optionally specify a maximum length (e.g., VARCHAR(255))
- **Space Efficient**: Only stores actual characters without padding
- **UTF-8 Encoding**: Supports full Unicode character sets
- **SQL Compliance**: Follows SQL standard behavior for VARCHAR types
- **Optional Type Modifier**: Can be used with or without length specification

The type is widely used in applications for storing text data where the length varies significantly.

## Parameters / Member Variables
As a typedef of ,  inherits:
- : Length and metadata field (do not access directly)
- : UTF-8 encoded character content

## Dependencies
- Functions called/Symbols referenced:
  - [varlena](../v/varlena.md) (base structure)
- Called from (representative examples):
  - [varcharin](../v/varcharin.md) (input function)
  - [varchar_input](../v/varchar_input.md) (input processing)
  - [varchar](../v/varchar.md) (type conversion/constraint checking)
  - [varcharrecv](../v/varcharrecv.md) (binary input)
  - [varcharsend](../v/varcharsend.md) (binary output)
  - [varchar_support](../v/varchar_support.md) (planner support function)
  - [varchartypmodin](../v/varchartypmodin.md) (type modifier input)
  - [varchartypmodout](../v/varchartypmodout.md) (type modifier output)

## Notes and Other Information
- **SQL VARCHAR(n) Semantics**: Implements exact SQL standard behavior for variable-length character types  
- **Length Enforcement**: When a maximum length is specified, longer strings are truncated with appropriate warnings
- **No Trailing Spaces**: Unlike CHAR(n), does not automatically add or remove trailing spaces
- **Storage Efficiency**: More space-efficient than CHAR(n) for strings shorter than the maximum length
- **Unlimited Length**: Can be declared without a length constraint (equivalent to TEXT in PostgreSQL)
- **Common Usage**: Widely used in database schemas for storing variable-length text data
- **Type Modifier Optional**: Can be used as VARCHAR (unlimited) or VARCHAR(n) (limited to n characters)
- **Performance**: Optimized for variable-length string operations and comparisons
- **Indexing**: Supports various index types including B-tree, Hash, and text search indexes
- **Collation Support**: Respects database and column-level collation settings for sorting and comparison