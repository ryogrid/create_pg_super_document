# to_tsvector_byid

## Location
[src/backend/tsearch/to_tsany.c:243-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L243-L269)

## Overview
A PostgreSQL built-in function that converts text to a tsvector using a specified text search configuration, serving as the core implementation for text-to-tsvector conversion.

## Definition


## Detailed Description
 is the main entry point for converting plain text into a tsvector data structure using a specific text search configuration. The function orchestrates the complete text-to-tsvector pipeline:

1. **Input processing**: Extracts the configuration OID and input text from PostgreSQL function arguments.

2. **Memory estimation**: Estimates the number of words in the input text (using a heuristic of 6 bytes per word on average) and allocates initial memory for the ParsedText structure.

3. **Bounds checking**: Ensures the estimated word count doesn't exceed PostgreSQL's maximum allocation size to prevent memory exhaustion.

4. **Text parsing**: Calls  with the specified configuration to tokenize and lexically analyze the input text, populating the ParsedText structure with words and their positions.

5. **TSVector construction**: Calls  to build the final binary TSVector representation from the parsed data.

6. **Memory cleanup**: Properly frees the input text copy if needed and returns the resulting TSVector.

The function handles the complete lifecycle from raw text input to structured tsvector output, making it the primary interface for text search preprocessing.

## Parameters / Member Variables
- Follows PostgreSQL function calling convention with :
  - Argument 0:  (Oid) - Text search configuration to use
  - Argument 1:  (text*) - Input text to convert

Returns:  containing the resulting TSVector

## Dependencies
- Functions called/Symbols referenced:
  - : PostgreSQL macro to extract OID argument
  - : PostgreSQL macro to extract text argument  
  - : Gets size of variable-length data excluding header
  - : Gets pointer to variable-length data
  - : PostgreSQL memory allocation
  - : Tokenizes and analyzes text using specified configuration
  - : Constructs final TSVector from parsed data
  - : Frees text argument if it was copied
  - : PostgreSQL macro to return TSVector result
- Called from (representative examples):
  - : Wrapper that uses default text search configuration

## Notes and Other Information
- This is the core function for PostgreSQL's  SQL function
- Uses a heuristic estimation (6 bytes per word) for initial memory allocation
- Implements proper memory management for both fixed and variable-sized allocations
- The configuration ID determines which parser and dictionaries are used for text analysis
- Located at lines 243-269 in 
- Part of PostgreSQL's full-text search infrastructure, enabling efficient text search operations
- Handles memory bounds checking to prevent allocation failures with very large texts