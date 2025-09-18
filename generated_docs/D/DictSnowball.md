# DictSnowball

## Location
src/backend/snowball/dict_snowball.c: 162 - 175

## Overview
A structure representing the runtime state and configuration of a Snowball stemmer dictionary instance, managing stemmer environments, stop word lists, and memory contexts for text processing operations.

## Definition


## Detailed Description
The  structure represents an active instance of a Snowball stemmer dictionary in PostgreSQL's text search system. It encapsulates all the state needed to perform stemming operations, including the Snowball environment, stop word filtering, character encoding management, and memory context isolation. The structure is designed to handle the complexities of text processing across different character encodings while maintaining memory efficiency through proper context management.

## Parameters / Member Variables
- : Pointer to the Snowball environment () that maintains the stemmer's internal state
- : Stop word list () containing words that should be filtered out during processing
- : Boolean flag indicating whether character encoding conversion is needed before and after stemming operations
- : Function pointer to the specific stemming algorithm for this dictionary instance
- : Memory context () used to isolate memory allocations made by the Snowball stemmer

## Dependencies
- Functions called/Symbols referenced:
  - StopList (structure for managing stop words)
- Called from (representative examples):
  - [locate_stem_module](../l/locate_stem_module.md) (function that creates and configures DictSnowball instances)
  - [dsnowball_init](../d/dsnowball_init.md) (initialization function for Snowball dictionaries)
  - [dsnowball_lexize](../d/dsnowball_lexize.md) (lexical analysis function that uses DictSnowball instances)

## Notes and Other Information
- The structure is designed to handle memory management challenges specific to Snowball stemmers, which allocate memory that persists between function calls
- The  member ensures that Snowball's memory allocations are properly isolated and can be cleaned up when the dictionary is no longer needed
- The  flag handles character encoding conversions that may be necessary when the database encoding differs from what the stemmer expects
- Each  instance is tied to a specific language stemmer through the  function pointer
- The structure supports stop word filtering in addition to stemming, providing comprehensive text processing capabilities