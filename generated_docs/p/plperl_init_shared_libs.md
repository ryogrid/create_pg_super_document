# plperl_init_shared_libs

## Location
[src/pl/plperl/plperl.c:2168-2179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2168-L2179)

## Overview
Initializes shared Perl libraries and XS (eXternal Subroutine) modules during Perl interpreter startup for PL/Perl.

## Definition
```c
static void plperl_init_shared_libs(pTHX)
```

## Detailed Description
This function is called during Perl interpreter initialization to register essential XS modules that provide the core functionality for PL/Perl. It registers the DynaLoader module, which enables dynamic loading of Perl extensions, and the PostgreSQL::InServer::Util module, which provides utility functions for PL/Perl operations. The function is passed as a callback to perl_parse() during interpreter setup, ensuring these critical modules are available before any user Perl code is executed. Note that the PostgreSQL::InServer::SPI module bootstrap is handled separately in select_perl_context().

## Parameters / Member Variables
- `pTHX`: Perl threading context parameter (standard Perl macro for thread-safe operations)

## Dependencies
- Functions called/Symbols referenced:
  - newXS (Perl API function for registering XS subroutines)
  - boot_DynaLoader (DynaLoader bootstrap function)
  - boot_PostgreSQL__InServer__Util (PostgreSQL utility module bootstrap)
- Called from (representative examples):
  - perl_parse (via function pointer during Perl interpreter initialization)

## Notes and Other Information
- Called during Perl interpreter startup via perl_parse() as an initialization callback
- Registers essential XS modules before user code execution
- The `__FILE__` macro provides the source file name for XS registration
- Located in src/pl/plperl/plperl.c:2168-2179
- Part of the critical initialization sequence for PL/Perl interpreters
- The PostgreSQL::InServer::SPI module is bootstrapped separately in select_perl_context() rather than here
- Essential for enabling dynamic loading capabilities and PostgreSQL-specific utility functions in Perl

## Simplified Source
```c
static void plperl_init_shared_libs(pTHX) {
    char *file = __FILE__;

    // Register DynaLoader for dynamic loading of Perl extensions
    newXS("DynaLoader::boot_DynaLoader", boot_DynaLoader, file);

    // Register PostgreSQL utility module
    newXS("PostgreSQL::InServer::Util::bootstrap",
          boot_PostgreSQL__InServer__Util, file);

    // Note: SPI module bootstrap is handled in select_perl_context()
}
```