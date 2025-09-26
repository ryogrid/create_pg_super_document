# json_manifest_finalize_version

## Location
src/common/parse_manifest.c: 596 - 623

## Overview
Performs additional parsing and validation of the manifest version field and invokes a callback to notify the caller about the version details.

## Definition


## Detailed Description
This function is called when a complete JSON object for a manifest has been parsed. It handles the final processing of the manifest version field, including:

1. **Version Parsing**: Converts the string representation of the version to an integer using 
2. **Validation**: Ensures the version is a valid integer and is one of the supported versions (1 or 2)
3. **Callback Invocation**: Calls the version callback function to notify the caller about the parsed version

The function is part of PostgreSQL's JSON manifest parsing infrastructure, typically used for backup and restore operations where manifest files describe the contents and metadata of backup sets.

## Parameters / Member Variables
- : Pointer to JsonManifestParseState structure containing the current parsing state, including the manifest_version string field and parsing context

## Dependencies
- Functions called/Symbols referenced:
  -  - converts string to 64-bit integer
  -  - handles parsing error reporting
  -  - parsing state structure
  -  - parsing context structure
- Called from (representative examples):
  -  - JSON scalar value processing function
  - Used in  structure

## Notes and Other Information
- This is a static function, only accessible within the parse_manifest.c file
- Requires that the  flag is set in the parse state (checked via Assert)
- Only supports manifest versions 1 and 2; other versions trigger a parse failure
- Uses PostgreSQL's internal  function for robust integer parsing with error detection
- Part of the incremental JSON parsing framework for handling large manifest files efficiently