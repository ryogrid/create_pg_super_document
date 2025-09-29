# set_stack_entry_domain

## Location
[src/backend/utils/error/elog.c:782-798](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L782-L798)

## Overview
Sets the internationalization domain for an error data stack entry, configuring both the main domain and context domain for proper message localization.

## Definition
```c
static void set_stack_entry_domain(ErrorData *edata, const char *domain)
```

## Detailed Description
This function configures the internationalization (i18n) domain settings for a given ErrorData structure. It sets both the primary domain and context domain fields to enable proper localization of error messages. If no specific domain is provided, it defaults to the backend's standard text domain using the PG_TEXTDOMAIN macro.

The function ensures that both the main error message domain and the context message domain are properly initialized, following the same pattern used by set_errcontext_domain(). This dual domain setup allows for different parts of an error message to be localized from different translation domains if needed.

## Parameters / Member Variables
- `edata`: ErrorData * - Pointer to the error data structure to configure
- `domain`: const char * - The internationalization domain to set, or NULL to use default

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (struct type)
  - PG_TEXTDOMAIN (macro for text domain specification)

- Called from (representative examples):
  - [errstart](../e/errstart.md) (src/backend/utils/error/elog.c:450)
  - [errsave_start](../e/errsave_start.md) (src/backend/utils/error/elog.c:662)

## Notes and Other Information
- The function is static and only used internally within the error handling subsystem
- When domain parameter is NULL, defaults to "postgres" text domain via PG_TEXTDOMAIN
- Both edata->domain and edata->context_domain are set to the same value for consistency
- The context_domain initialization follows the same pattern as set_errcontext_domain() function
- This is part of PostgreSQL's internationalization infrastructure for error messages

## Simplified Source

```c
static void
set_stack_entry_domain(ErrorData *edata, const char *domain)
{
    // Set main domain (default to postgres backend domain)
    edata->domain = domain ? domain : PG_TEXTDOMAIN("postgres");

    // Initialize context domain the same way
    edata->context_domain = edata->domain;
}
```