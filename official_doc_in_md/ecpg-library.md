34.11. Library Functions  
---  
[Prev](ecpg-process.md "34.10. Processing Embedded SQL Programs") | [Up](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")| Chapter 34. ECPG — Embedded SQL in C| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-lo.md "34.12. Large Objects")  
  
* * *

## 34.11. Library Functions #

The `libecpg` library primarily contains “hidden” functions that are used to implement the functionality expressed by the embedded SQL commands. But there are some functions that can usefully be called directly. Note that this makes your code unportable. 

  * `ECPGdebug(int _`on`_ , FILE *_`stream`_)` turns on debug logging if called with the first argument non-zero. Debug logging is done on _`stream`_. The log contains all SQL statements with all the input variables inserted, and the results from the PostgreSQL server. This can be very useful when searching for errors in your SQL statements. 

### Note

On Windows, if the ecpg libraries and an application are compiled with different flags, this function call will crash the application because the internal representation of the `FILE` pointers differ. Specifically, multithreaded/single-threaded, release/debug, and static/dynamic flags should be the same for the library and all applications using that library. 

  * `ECPGget_PGconn(const char *_`connection_name`_) ` returns the library database connection handle identified by the given name. If _`connection_name`_ is set to `NULL`, the current connection handle is returned. If no connection handle can be identified, the function returns `NULL`. The returned connection handle can be used to call any other functions from libpq, if necessary. 

### Note

It is a bad idea to manipulate database connection handles made from ecpg directly with libpq routines. 

  * `ECPGtransactionStatus(const char *_`connection_name`_)` returns the current transaction status of the given connection identified by _`connection_name`_. See [Section 32.2](libpq-status.md "32.2. Connection Status Functions") and libpq's [`PQtransactionStatus`](libpq-status.md#LIBPQ-PQTRANSACTIONSTATUS) for details about the returned status codes. 

  * `ECPGstatus(int _`lineno`_ , const char* _`connection_name`_)` returns true if you are connected to a database and false if not. _`connection_name`_ can be `NULL` if a single connection is being used. 




* * *

[Prev](ecpg-process.md "34.10. Processing Embedded SQL Programs") | [Up](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")|  [Next](ecpg-lo.md "34.12. Large Objects")  
---|---|---  
34.10. Processing Embedded SQL Programs | [Home](index.md "PostgreSQL 17.5 Documentation")|  34.12. Large Objects
