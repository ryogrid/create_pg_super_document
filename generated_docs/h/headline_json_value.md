# headline_json_value

## Location
[src/backend/tsearch/wparser.c:523-542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L523-L542)

## Overview
A static helper function that generates text search headlines from JSON/JSONB element values by parsing the text and highlighting query matches.

## Definition


## Detailed Description
The  function serves as a callback handler for processing individual JSON/JSONB element values during text search headline generation. It takes a JSON element's string value and transforms it into a formatted headline text that highlights words matching a given TSQuery. The function operates within the context of a  structure that contains all necessary configuration and state information for the headline generation process.

The function performs the following key operations:
1. Extracts state information from the provided  structure
2. Resets the word counter for the current parsing session
3. Parses the JSON element text using the configured text search parser ()
4. Applies headline formatting using the parser's headline function
5. Generates the final formatted headline text with query match highlighting
6. Marks the state as transformed to indicate successful processing

This function is specifically designed to work with PostgreSQL's full-text search functionality for JSON/JSONB data types, enabling users to generate search result snippets from JSON content.

## Parameters / Member Variables
- : A void pointer that is cast to , containing all the configuration and state needed for headline generation
- : A character pointer to the JSON element's string value that needs to be processed
- : An integer specifying the length of the element value in bytes

## Dependencies
- Functions called/Symbols referenced:
  - : Parses text content using text search configuration to identify and classify words
  - : PostgreSQL's function call mechanism to invoke the parser's headline function
  - : Generates the final formatted headline text with highlighting
  - : Converts pointers to PostgreSQL Datum format for function calls

- Called from (representative examples):
  - : Main function for generating headlines from JSON with explicit config
  - : Main function for generating headlines from JSONB with explicit config

## Notes and Other Information
- This is a static function, meaning it's only accessible within the  compilation unit
- The function is designed to work with PostgreSQL's JSON/JSONB data types specifically
- The  flag in the state is set to  to indicate successful processing
- The function relies on the TSParser framework for text analysis and the TSQuery system for match detection
- Word counting is reset () at the beginning of each call to ensure clean state
- The function integrates with PostgreSQL's headline generation system, which formats text by adding start/stop selection markers around matching words
- Memory management for the returned text object is handled by PostgreSQL's memory context system