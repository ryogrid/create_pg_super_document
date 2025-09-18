# pg_xml_init_library

## Location
src/backend/utils/adt/xml.c: 1165 - 1210

## Overview
The pg_xml_init_library function performs one-time per-session initialization of the libxml2 library, including compatibility checks and memory allocation setup.

## Definition
```c
void pg_xml_init_library(void)
```

## Detailed Description
This function initializes the libxml2 library for use within PostgreSQL and performs essential compatibility checks. It uses a static flag to ensure initialization occurs only once per session. The function verifies that the char and xmlChar types have compatible sizes, which is crucial for proper XML processing. When USE_LIBXMLCONTEXT is defined, it sets up custom memory allocation routines for libxml2 to integrate with PostgreSQL's memory management. Finally, it performs library compatibility verification using libxml2's built-in version checking mechanism.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting)
  - [errmsg](../e/errmsg.md) (error message formatting)  
  - [errdetail](../e/errdetail.md) (error detail formatting)
  - xml_memory_init (custom memory allocation setup, conditional)
  - LIBXML_TEST_VERSION (libxml2 compatibility check macro)
- Called from (representative examples):
  - [pg_xml_init](pg_xml_init.md) (higher-level XML initialization with error handling)
  - parse_xml_decl (XML declaration parsing)

## Notes and Other Information
- This is a one-time per-session initialization function controlled by a static boolean flag
- Performs critical compatibility checking between PostgreSQL's char type and libxml2's xmlChar type
- The function will throw an error if char and xmlChar types have different sizes, indicating incompatible libxml2 build
- When USE_LIBXMLCONTEXT is enabled, integrates libxml2's memory allocation with PostgreSQL's memory management
- Should be called by functions that need libxml2 but don't require error handling setup
- For functions requiring error handling, use pg_xml_init() instead which calls this function internally
- The LIBXML_TEST_VERSION macro performs runtime compatibility verification with the libxml2 library