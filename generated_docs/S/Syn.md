# Syn

## Location
src/backend/tsearch/dict_synonym.c: 27 - 33

## Overview
Syn is a structure that represents a single synonym mapping entry in PostgreSQL's text search synonym dictionary functionality.

## Definition


## Detailed Description
The Syn structure is a fundamental data type used by PostgreSQL's synonym dictionary (dict_synonym) for text search operations. Each Syn instance represents a single synonym mapping rule that transforms an input word to an output word. The structure is designed to store both the original word and its synonym replacement, along with metadata about the replacement including its length and any special flags.

This structure is primarily used within the context of full-text search operations where users want to define custom synonym mappings. For example, mapping "TV" to "television" or "NYC" to "New York City". The synonym dictionary loads these mappings from configuration files and stores them as arrays of Syn structures for efficient lookup during text search operations.

## Parameters / Member Variables
- : Pointer to the input word string (the word to be replaced)
- : Pointer to the output word string (the replacement/synonym word)
- : Length of the output string, stored for efficiency to avoid repeated strlen() calls
- : 16-bit flags field to store metadata about the synonym mapping (e.g., case sensitivity, special processing flags)

## Dependencies
- Functions called/Symbols referenced:
  - Used in DictSyn structure as an array element
- Called from (representative examples):
  - [compareSyn](../c/compareSyn.md) (for sorting/searching synonym arrays)
  - [dsynonym_init](../d/dsynonym_init.md) (during dictionary initialization from config files)
  - [dsynonym_lexize](../d/dsynonym_lexize.md) (during actual text search lexicalization)

## Notes and Other Information
- The Syn structures are typically stored in sorted arrays within DictSyn for efficient binary search operations
- Memory management for the  and  strings is handled by the calling dictionary functions
- The structure is designed for read-heavy workloads where synonym lookups happen frequently during text search operations
- Case sensitivity of synonym matching is controlled at the dictionary level (DictSyn.case_sensitive) rather than per-synonym
- The flags field provides extensibility for future enhancements to synonym functionality