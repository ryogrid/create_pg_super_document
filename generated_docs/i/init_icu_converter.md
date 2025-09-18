# init_icu_converter

## Location
src/backend/utils/adt/pg_locale.c: 2684 - 2713

## Overview
Initializes the ICU character encoding converter for database operations by setting up a UConverter instance for the current database encoding.

## Definition
static void init_icu_converter(void)

## Detailed Description
This function initializes the global ICU converter (icu_converter) that is used for character encoding conversions between PostgreSQL's internal database encoding and ICU's Unicode representation. The function:

1. Checks if the converter is already initialized to avoid redundant initialization
2. Maps the database encoding to an ICU-compatible encoding name
3. Creates a UConverter instance using ICU's ucnv_open() function
4. Handles errors by reporting appropriate PostgreSQL error messages
5. Stores the converter in the global icu_converter variable

The function is essential for ICU-based collation and string transformation operations, ensuring that text data can be properly converted between PostgreSQL's encoding and ICU's internal Unicode representation.

## Parameters / Member Variables
This function takes no parameters and returns void.

## Dependencies
- Functions called/Symbols referenced:
  - get_encoding_name_for_icu (maps PostgreSQL encoding to ICU encoding name)
  - GetDatabaseEncoding (gets current database encoding)
  - pg_encoding_to_char (converts encoding ID to string for error messages)
  - ucnv_open (ICU function to create converter)
  - u_errorName (ICU function to get error name string)
- Called from (representative examples):
  - collation_cache_entry (during collation initialization)
  - pg_strncoll_icu_no_utf8 (string collation operations)
  - pg_strnxfrm_icu (string transformation operations)
  - pg_strnxfrm_prefix_icu_no_utf8 (prefix transformation operations)
  - icu_to_uchar (character conversion to Unicode)
  - icu_from_uchar (character conversion from Unicode)

## Notes and Other Information
- This is a static function, only accessible within the pg_locale.c file
- The function uses lazy initialization - the converter is only created when first needed
- Error handling follows PostgreSQL conventions using ereport() with appropriate error codes
- The global icu_converter variable is used to store the initialized converter for reuse
- This function is critical for ICU-based internationalization features in PostgreSQL
- The converter remains valid for the lifetime of the backend process