# provider_init

## Location
[src/backend/jit/jit.c:67-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/jit.c#L67-L126)

## Overview
A static function that manages the loading and initialization of the JIT provider, caching the result to avoid repeated expensive loading attempts.

## Definition
```c
static bool provider_init(void)
```

## Detailed Description
This function handles the core logic for JIT provider initialization in PostgreSQL. It implements a caching mechanism to avoid repeatedly attempting to load the JIT provider, which is an expensive operation. The function first checks if JIT is enabled, then verifies whether the provider has already been loaded (successfully or unsuccessfully). If the provider hasn't been loaded yet, it constructs the path to the JIT provider shared library, checks for its existence, and attempts to load and initialize it. The function uses several static variables to track the loading state and avoid redundant attempts.

## Parameters / Member Variables
This function takes no parameters but uses several key variables:
- `path[MAXPGPATH]`: Buffer to construct the path to the JIT provider shared library
- `init`: Function pointer of type `JitProviderInit` used to initialize the loaded provider
- Uses global variables: `jit_enabled`, `provider_failed_loading`, `provider_successfully_loaded`, `pkglib_path`, `jit_provider`

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - elog (with DEBUG1 level)
  - [pg_file_exists](pg_file_exists.md)
  - [load_external_function](../l/load_external_function.md)
  - [_PG_jit_provider_init](../P/_PG_jit_provider_init.md) (dynamically loaded function)
- Called from (representative examples):
  - [pg_jit_available](pg_jit_available.md)
  - [jit_compile_expr](../j/jit_compile_expr.md)

## Notes and Other Information
- Located in src/backend/jit/jit.c:67-126
- Implements a three-state caching system: unknown, failed, or successful
- The function constructs the provider library path using `pkglib_path`, `jit_provider`, and `DLSUFFIX`
- Sets `provider_failed_loading` to true before attempting to load to handle potential errors during loading
- Uses DEBUG1 logging level to provide information about JIT provider loading attempts
- The loaded provider is stored in a global `provider` variable for later use