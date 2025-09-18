# conninfo_storeval

## Location
src/interfaces/libpq/fe-connect.c: 6864 - 6927

## Overview
A static function that stores a new value for a connection option in the connOptions array, with support for URI decoding and backward compatibility handling for legacy SSL options.

## Definition


## Detailed Description
This function updates or sets a connection option value in the PostgreSQL connection options array. It performs several key operations:

1. **Backward Compatibility**: Handles legacy `requiressl` parameter by translating it to the modern `sslmode` parameter (requiressl=1 becomes sslmode=require, requiressl=0 becomes sslmode=prefer)
2. **Option Lookup**: Uses `conninfo_find` to locate the target option in the connOptions array
3. **URI Decoding**: Optionally decodes URI-encoded values when `uri_decode` is true
4. **Memory Management**: Properly frees existing option values before setting new ones to prevent memory leaks
5. **Error Handling**: Provides detailed error messages for invalid keywords and memory allocation failures

The function ensures that connection option values are properly stored with appropriate memory management and backward compatibility support.

## Parameters / Member Variables
- `connOptions`: Array of PQconninfoOption structures representing all available connection options
- `keyword`: The name of the connection option to set (non URI-encoded)
- `value`: The new value to store for the option
- `errorMessage`: Buffer for storing error messages when operations fail
- `ignoreMissing`: If true, silently ignore attempts to set invalid/unknown keywords
- `uri_decode`: If true, URI-decode the value before storing it

## Dependencies
- Functions called/Symbols referenced:
  - conninfo_find
  - conninfo_uri_decode  
  - libpq_append_error
  - strdup
  - free
  - strcmp
- Called from (representative examples):
  - internalPQconninfoOption
  - conninfo_parse
  - conninfo_uri_parse_options
  - conninfo_uri_parse_params
  - PQconninfo

## Notes and Other Information
- This is a static internal function not exposed in the public libpq API
- Provides backward compatibility for the deprecated `requiressl` parameter
- Handles memory management carefully by freeing existing values before setting new ones
- Returns NULL on failure with appropriate error messages, or a pointer to the updated PQconninfoOption on success
- The function is central to PostgreSQL's connection string parsing and option management system