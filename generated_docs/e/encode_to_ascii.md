# encode_to_ascii

## Location
[src/backend/utils/adt/ascii.c:104-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ascii.c#L104-L118)

## Overview
A static function that performs in-place ASCII encoding conversion on text data by calling the underlying pg_to_ascii function.

## Definition
```c
static text *encode_to_ascii(text *data, int enc)
```

## Detailed Description
This function serves as a wrapper around the pg_to_ascii function to convert text data to ASCII encoding. The key characteristic of this function is that it performs the conversion in-place, meaning it overwrites the original text datum directly rather than creating a new copy. This design choice means the function cannot support conversions that would change the string length, as noted in the source comments.

The function extracts the variable-length data from the PostgreSQL text type using VARDATA and VARSIZE macros, then passes the source data, destination buffer (same as source for in-place operation), and encoding type to pg_to_ascii for the actual conversion work.

## Parameters / Member Variables
- `data`: PostgreSQL text datum containing the string data to be converted to ASCII
- `enc`: Integer encoding identifier specifying the source encoding type

## Dependencies
- Functions called/Symbols referenced:
  - [pg_to_ascii](../p/pg_to_ascii.md) (performs the actual ASCII conversion)
  - VARDATA (macro to access variable-length data)
  - VARSIZE (macro to get size of variable-length data)
- Called from:
  - [to_ascii_encname](../t/to_ascii_encname.md)
  - [to_ascii_enc](../t/to_ascii_enc.md)  
  - [to_ascii_default](../t/to_ascii_default.md)

## Notes and Other Information
- This is a static function, only accessible within the ascii.c file
- Performs in-place conversion, modifying the original data
- Cannot handle conversions that change string length due to in-place design
- Returns the same text pointer that was passed in (modified in-place)

## Simplified Source

```c
static text *encode_to_ascii(text *data, int enc) {
    // Convert text to ASCII in-place using the specified encoding
    // Source and destination are the same (in-place conversion)
    pg_to_ascii((unsigned char *) VARDATA(data),           // source data
               (unsigned char *) (data) + VARSIZE(data),    // source end
               (unsigned char *) VARDATA(data),             // destination (same as source)
               enc);                                        // encoding type

    return data;
}
```

This function performs in-place ASCII conversion by calling `pg_to_ascii()` with the same buffer as both source and destination. The conversion cannot change string length since it modifies the data in place. It extracts the text data using PostgreSQL's variable-length data macros.