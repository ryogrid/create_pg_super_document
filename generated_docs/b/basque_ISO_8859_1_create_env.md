# basque_ISO_8859_1_create_env

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c:1179-1180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c#L1179-L1180)

## Overview
Factory function that creates and initializes a new Snowball environment structure for Basque ISO-8859-1 stemming operations.

## Definition
extern struct SN_env * basque_ISO_8859_1_create_env(void)

## Detailed Description
This function serves as a constructor for the Basque stemming environment using ISO-8859-1 character encoding. It creates a new SN_env structure by calling the generic SN_create_env function with parameters specific to Basque language requirements. The function allocates memory and initializes the necessary data structures for performing Basque stemming operations, including buffer space and region markers.

## Parameters / Member Variables
- None (void function)
- Returns: Pointer to newly created SN_env structure configured for Basque stemming

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md) (generic Snowball environment creator with parameters 0, 3)
- Called from (representative examples):
  - Client code initializing Basque stemming functionality

## Notes and Other Information
This is a thin wrapper around the generic Snowball environment creation function. The parameters (0, 3) passed to SN_create_env indicate specific configuration values for the Basque language stemmer. Memory management responsibility transfers to the caller, who must eventually call the corresponding close_env function to prevent memory leaks.