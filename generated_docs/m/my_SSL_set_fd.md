# my_SSL_set_fd

## Location
src/interfaces/libpq/fe-secure-openssl.c: 2038 - 2070

## Overview
Associates a file descriptor with an SSL connection using PostgreSQL's custom BIO method, replacing OpenSSL's standard SSL_set_fd function.

## Definition


## Detailed Description
This function serves as a PostgreSQL-specific replacement for OpenSSL's SSL_set_fd function. Instead of using OpenSSL's default socket BIO, it creates a BIO using PostgreSQL's custom my_BIO_s_socket method. This allows PostgreSQL to maintain control over socket I/O operations while still benefiting from OpenSSL's SSL/TLS functionality.

The function creates a new BIO instance using the custom PostgreSQL BIO method, associates it with the provided file descriptor, and sets the Port structure as application data. This enables the custom read/write callbacks (my_sock_read and my_sock_write) to access PostgreSQL's connection context during SSL operations.

## Parameters / Member Variables
- : PostgreSQL Port structure containing SSL connection state and context
- : File descriptor for the socket connection

## Dependencies
- Functions called/Symbols referenced:
  - [my_BIO_s_socket](my_BIO_s_socket.md)
  - BIO_new
  - BIO_set_app_data
  - BIO_set_fd
  - SSL_set_bio
  - SSLerr (error reporting macro)
- Called from (representative examples):
  - [be_tls_open_server](../b/be_tls_open_server.md)
  - [initialize_SSL](../i/initialize_SSL.md)

## Notes and Other Information
- This is a static function used internally within PostgreSQL's OpenSSL integration
- Mimics OpenSSL's SSL_set_fd behavior but uses PostgreSQL's custom BIO method
- Returns 1 on success, 0 on failure
- The BIO is set for both read and write operations on the same SSL object
- Uses BIO_NOCLOSE flag to prevent BIO from closing the file descriptor
- Essential for integrating PostgreSQL's connection management with SSL/TLS
- Enables PostgreSQL to handle non-blocking I/O and connection state properly during SSL operations
- Part of the backend SSL implementation for secure database server connections