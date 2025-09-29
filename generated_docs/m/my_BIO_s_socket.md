# my_BIO_s_socket

## Location
[src/interfaces/libpq/fe-secure-openssl.c:1972-2037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L1972-L2037)

## Overview
Creates and returns a custom BIO_METHOD structure for PostgreSQL's secure socket communication with OpenSSL, providing PostgreSQL-specific socket I/O operations.

## Definition

```c
static BIO_METHOD *
my_BIO_s_socket(void)
```
## Detailed Description
This function creates a custom BIO method structure that integrates PostgreSQL's secure socket I/O operations with OpenSSL's BIO abstraction layer. It uses a singleton pattern to ensure only one instance of the custom BIO method is created. The function handles two different OpenSSL API versions - newer versions with BIO_meth_new() and older versions that require direct structure manipulation.

The custom BIO method replaces the default socket read and write operations with PostgreSQL's my_sock_read and my_sock_write functions, while inheriting other standard socket BIO operations from the default socket BIO. This allows PostgreSQL to maintain control over socket I/O operations while leveraging OpenSSL's SSL/TLS functionality.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - BIO_s_socket
  - BIO_get_new_index
  - BIO_meth_new
  - BIO_meth_set_write
  - BIO_meth_set_read
  - BIO_meth_get_gets
  - BIO_meth_get_puts
  - BIO_meth_get_ctrl
  - BIO_meth_get_create
  - BIO_meth_get_destroy
  - BIO_meth_get_callback_ctrl
  - BIO_meth_free
  - malloc
  - memcpy
  - [my_sock_write](my_sock_write.md)
  - [my_sock_read](my_sock_read.md)
- Called from (representative examples):
  - [my_SSL_set_fd](my_SSL_set_fd.md)

## Notes and Other Information
- Uses a static global variable my_bio_methods to implement singleton pattern
- Supports both modern OpenSSL API (with HAVE_BIO_METH_NEW) and legacy API
- Creates a BIO method named "PostgreSQL backend socket" with custom type flags
- Inherits most BIO operations from the standard socket BIO but overrides read/write operations
- Essential for integrating PostgreSQL's connection management with OpenSSL's SSL/TLS layer
- Returns NULL on any failure during BIO method creation or configuration
- Part of PostgreSQL's custom SSL implementation for secure database connections

## Simplified Source

```c
static BIO_METHOD *my_BIO_s_socket(void)
{
    // Singleton pattern - create BIO method only once
    if (!my_bio_methods)
    {
        BIO_METHOD *biom = (BIO_METHOD *) BIO_s_socket();

#ifdef HAVE_BIO_METH_NEW
        // Modern OpenSSL API (1.1.0+)
        int my_bio_index;

        // Get a new BIO type index
        my_bio_index = BIO_get_new_index();
        if (my_bio_index == -1)
            return NULL;

        // Set BIO type flags
        my_bio_index |= (BIO_TYPE_DESCRIPTOR | BIO_TYPE_SOURCE_SINK);

        // Create new BIO method with PostgreSQL identifier
        my_bio_methods = BIO_meth_new(my_bio_index, "PostgreSQL backend socket");
        if (!my_bio_methods)
            return NULL;

        // Set up custom read/write operations and inherit standard operations
        if (!BIO_meth_set_write(my_bio_methods, my_sock_write) ||
            !BIO_meth_set_read(my_bio_methods, my_sock_read) ||
            !BIO_meth_set_gets(my_bio_methods, BIO_meth_get_gets(biom)) ||
            !BIO_meth_set_puts(my_bio_methods, BIO_meth_get_puts(biom)) ||
            !BIO_meth_set_ctrl(my_bio_methods, BIO_meth_get_ctrl(biom)) ||
            !BIO_meth_set_create(my_bio_methods, BIO_meth_get_create(biom)) ||
            !BIO_meth_set_destroy(my_bio_methods, BIO_meth_get_destroy(biom)) ||
            !BIO_meth_set_callback_ctrl(my_bio_methods, BIO_meth_get_callback_ctrl(biom)))
        {
            // Clean up on failure
            BIO_meth_free(my_bio_methods);
            my_bio_methods = NULL;
            return NULL;
        }

#else
        // Legacy OpenSSL API (pre-1.1.0)
        my_bio_methods = malloc(sizeof(BIO_METHOD));
        if (!my_bio_methods)
            return NULL;

        // Copy standard socket BIO method and override read/write functions
        memcpy(my_bio_methods, biom, sizeof(BIO_METHOD));
        my_bio_methods->bread = my_sock_read;
        my_bio_methods->bwrite = my_sock_write;
#endif
    }

    return my_bio_methods;
}
```