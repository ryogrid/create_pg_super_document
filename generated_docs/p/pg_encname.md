# pg_encname

## Location
[src/common/encnames.c:33-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/encnames.c#L33-L37)

## Overview
A structure that maps character encoding names to PostgreSQL encoding identifiers, used in the encoding name lookup table for character set conversion operations.

## Definition

```c
typedef struct pg_encname
{
	const char *name;
	pg_enc		encoding;
} pg_encname;
```
## Detailed Description
The  structure serves as an entry in PostgreSQL's encoding name lookup table (). This structure is fundamental to PostgreSQL's character encoding system, providing a mapping between human-readable encoding names (like "utf8", "iso88591") and their corresponding internal encoding identifiers.

The structure is used primarily in binary search operations within the  function to efficiently locate encoding IDs based on encoding names. All encoding names in the table are normalized (lowercase, alphanumeric characters only) and stored in alphabetical order to enable efficient binary search lookup.

The design follows PostgreSQL's approach of maintaining a static, read-only lookup table that maps various encoding name formats and aliases to standardized internal encoding identifiers, facilitating consistent character set handling across the system.

## Parameters / Member Variables
- `*name`: A normalized encoding name string (lowercase, alphanumeric only) that serves as the lookup key. Examples include "utf8", "iso88591", "eucjp"
- `encoding`: The corresponding PostgreSQL encoding identifier of type  (an enumerated value) that represents the internal encoding ID
## Dependencies
- Functions called/Symbols referenced:
  - [pg_enc](pg_enc.md) (enumerated type)
- Called from (representative examples):
  - [pg_char_to_encoding](pg_char_to_encoding.md) (uses pg_encname_tbl array of this structure)

## Notes and Other Information
- All encoding names must be preprocessed to remove irrelevant characters (hyphens, underscores) and converted to lowercase before storage in the table
- The structure is used exclusively in a static array () that is kept in strict alphabetical order for binary search efficiency
- The table serves as the authoritative mapping for encoding name resolution in PostgreSQL's character set conversion system
- Originally implemented by Karel Zak in August 2001 as part of PostgreSQL's encoding infrastructure