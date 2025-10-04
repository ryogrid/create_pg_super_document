# __ubsan_default_options

## Location
[src/backend/main/main.c:438-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/main/main.c#L438-L445)

## Overview
A weak symbol function that provides default options for the Undefined Behavior Sanitizer (UBSan) runtime library, allowing PostgreSQL to work around sanitizer initialization issues caused by process title changes.

## Definition
```c
const char *__ubsan_default_options(void)
```

## Detailed Description
This function serves as a workaround for a compatibility issue between PostgreSQL's `set_ps_display()` function and the UBSan sanitizer library on Linux systems. The problem occurs because `set_ps_display()` modifies `/proc/$pid/environ`, which the sanitizer library relies on to implement `getenv()` independently of libc. Since sanitizers are often initialized only when the first error occurs (after `set_ps_display()` has been called), the sanitizer library cannot see the `UBSAN_OPTIONS` environment variable.

The function implements a weak symbol that libsanitizer recognizes and uses to obtain default configuration options from the application. It safely returns the value of the `UBSAN_OPTIONS` environment variable, but only after the main function has been reached to ensure that libc is properly initialized.

The function includes a safety check using the `reached_main` static variable to prevent calling `getenv()` before libc is guaranteed to be working properly. If called before main initialization, it returns an empty string instead.

## Parameters / Member Variables
This function takes no parameters and returns:
- **Return value**: A pointer to a constant character string containing either the value of the `UBSAN_OPTIONS` environment variable or an empty string if called before main initialization.

## Dependencies
- Functions called/Symbols referenced:
  - `getenv()` - Standard C library function to retrieve environment variables
  - `reached_main` - Static boolean variable tracking main function initialization state
- Called from (representative examples):
  - libsanitizer runtime (external library, called automatically by UBSan when needed)

## Notes and Other Information
- This function is compiled unconditionally since it will only be called when running with sanitizers enabled
- The function is defined as a weak symbol, meaning it can be overridden by stronger symbol definitions
- Located in `src/backend/main/main.c` at lines 438-445
- The `reached_main` variable is set to true early in the main function initialization process
- This workaround specifically addresses issues on Linux systems where `/proc/$pid/environ` access is affected by process title changes
- The function provides a bridge between PostgreSQL's initialization process and the sanitizer library's configuration needs

## Simplified Source
```c
const char *__ubsan_default_options(void) {
    // Don't call libc before it's guaranteed to be initialized
    if (!reached_main)
        return "";

    // Return UBSAN_OPTIONS environment variable value
    return getenv("UBSAN_OPTIONS");
}
```