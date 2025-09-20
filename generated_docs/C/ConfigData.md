# ConfigData

## Location
[src/include/common/config_info.h:12-16](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/config_info.h#L12-L16)

## Overview
ConfigData is a simple structure that holds PostgreSQL build and installation configuration information as name-value pairs for use by the pg_config utility and related configuration reporting functions.

## Definition

```c
typedef struct ConfigData
{
	char	   *name;
	char	   *setting;
} ConfigData;
```
## Detailed Description
ConfigData serves as a fundamental data structure for storing and retrieving PostgreSQL's build-time and installation configuration information. It is primarily used by the BINDIR = /usr/lib/postgresql/14/bin
DOCDIR = /usr/share/doc/postgresql-doc-14
HTMLDIR = /usr/share/doc/postgresql-doc-14
INCLUDEDIR = /usr/include/postgresql
PKGINCLUDEDIR = /usr/include/postgresql
INCLUDEDIR-SERVER = /usr/include/postgresql/14/server
LIBDIR = /usr/lib/x86_64-linux-gnu
PKGLIBDIR = /usr/lib/postgresql/14/lib
LOCALEDIR = /usr/share/locale
MANDIR = /usr/share/postgresql/14/man
SHAREDIR = /usr/share/postgresql/14
SYSCONFDIR = /etc/postgresql-common
PGXS = /usr/lib/postgresql/14/lib/pgxs/src/makefiles/pgxs.mk
CONFIGURE =  '--build=x86_64-linux-gnu' '--prefix=/usr' '--includedir=${prefix}/include' '--mandir=${prefix}/share/man' '--infodir=${prefix}/share/info' '--sysconfdir=/etc' '--localstatedir=/var' '--disable-option-checking' '--disable-silent-rules' '--libdir=${prefix}/lib/x86_64-linux-gnu' '--runstatedir=/run' '--disable-maintainer-mode' '--disable-dependency-tracking' '--with-tcl' '--with-perl' '--with-python' '--with-pam' '--with-openssl' '--with-libxml' '--with-libxslt' '--mandir=/usr/share/postgresql/14/man' '--docdir=/usr/share/doc/postgresql-doc-14' '--sysconfdir=/etc/postgresql-common' '--datarootdir=/usr/share/' '--datadir=/usr/share/postgresql/14' '--bindir=/usr/lib/postgresql/14/bin' '--libdir=/usr/lib/x86_64-linux-gnu/' '--libexecdir=/usr/lib/postgresql/' '--includedir=/usr/include/postgresql/' '--with-extra-version= (Ubuntu 14.17-0ubuntu0.22.04.1)' '--enable-nls' '--enable-thread-safety' '--enable-debug' '--enable-dtrace' '--disable-rpath' '--with-uuid=e2fs' '--with-gnu-ld' '--with-gssapi' '--with-ldap' '--with-pgport=5432' '--with-system-tzdata=/usr/share/zoneinfo' 'AWK=mawk' 'MKDIR_P=/bin/mkdir -p' 'PROVE=/usr/bin/prove' 'PYTHON=/usr/bin/python3' 'TAR=/bin/tar' 'XSLTPROC=xsltproc --nonet' 'CFLAGS=-g -O2 -flto=auto -ffat-lto-objects -flto=auto -ffat-lto-objects -fstack-protector-strong -Wformat -Werror=format-security -fno-omit-frame-pointer' 'LDFLAGS=-Wl,-Bsymbolic-functions -flto=auto -ffat-lto-objects -flto=auto -Wl,-z,relro -Wl,-z,now' '--enable-tap-tests' '--with-icu' '--with-llvm' 'LLVM_CONFIG=/usr/bin/llvm-config-14' 'CLANG=/usr/bin/clang-14' '--with-lz4' '--with-systemd' '--with-selinux' 'build_alias=x86_64-linux-gnu' 'CPPFLAGS=-Wdate-time -D_FORTIFY_SOURCE=2' 'CXXFLAGS=-g -O2 -flto=auto -ffat-lto-objects -flto=auto -ffat-lto-objects -fstack-protector-strong -Wformat -Werror=format-security'
CC = gcc
CPPFLAGS = -Wdate-time -D_FORTIFY_SOURCE=2 -D_GNU_SOURCE -I/usr/include/libxml2
CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Werror=vla -Wendif-labels -Wmissing-format-attribute -Wimplicit-fallthrough=3 -Wcast-function-type -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -Wno-format-truncation -Wno-stringop-truncation -g -g -O2 -flto=auto -ffat-lto-objects -flto=auto -ffat-lto-objects -fstack-protector-strong -Wformat -Werror=format-security -fno-omit-frame-pointer
CFLAGS_SL = -fPIC
LDFLAGS = -Wl,-Bsymbolic-functions -flto=auto -ffat-lto-objects -flto=auto -Wl,-z,relro -Wl,-z,now -L/usr/lib/llvm-14/lib -Wl,--as-needed
LDFLAGS_EX = 
LDFLAGS_SL = 
LIBS = -lpgcommon -lpgport -lselinux -llz4 -lxslt -lxml2 -lpam -lssl -lcrypto -lgssapi_krb5 -lz -lreadline -lm 
VERSION = PostgreSQL 14.17 (Ubuntu 14.17-0ubuntu0.22.04.1) utility and related configuration reporting mechanisms to provide standardized access to various PostgreSQL installation paths, compiler settings, and build options.

The structure acts as a key-value container where configuration parameter names (like "BINDIR", "LIBDIR", "VERSION") are paired with their corresponding values (actual paths or settings). This design allows for flexible storage and retrieval of configuration data without requiring separate variables for each configuration item.

The ConfigData structure is typically used in arrays, with functions like  populating multiple ConfigData entries to represent all available configuration information. The pg_config utility uses this data to respond to command-line queries about PostgreSQL's installation and build configuration.

## Parameters / Member Variables
- `*name`: A string containing the configuration parameter name (e.g., "BINDIR", "VERSION", "CC", "CONFIGURE")
- `*setting`: A string containing the corresponding configuration value (e.g., actual directory paths, compiler flags, or version information)
## Dependencies
- Functions called/Symbols referenced: None (simple struct definition)
- Called from (representative examples):
  - [get_configdata](../g/get_configdata.md) (src/common/config_info.c:35, 42)
  - [show_item](../s/show_item.md) (src/bin/pg_config/pg_config.c:117)
  - [main](../m/main.md) (src/bin/pg_config/pg_config.c:132)
  - [pg_config](../p/pg_config.md) (src/backend/utils/misc/pg_config.c:27)

## Notes and Other Information
- Defined in  as part of the common configuration information interface
- Memory for both  and  fields is typically allocated using PostgreSQL's memory management functions (, )
- The structure is designed to be platform-independent and works in both frontend utilities and backend code
- Used extensively by the BINDIR = /usr/lib/postgresql/14/bin
DOCDIR = /usr/share/doc/postgresql-doc-14
HTMLDIR = /usr/share/doc/postgresql-doc-14
INCLUDEDIR = /usr/include/postgresql
PKGINCLUDEDIR = /usr/include/postgresql
INCLUDEDIR-SERVER = /usr/include/postgresql/14/server
LIBDIR = /usr/lib/x86_64-linux-gnu
PKGLIBDIR = /usr/lib/postgresql/14/lib
LOCALEDIR = /usr/share/locale
MANDIR = /usr/share/postgresql/14/man
SHAREDIR = /usr/share/postgresql/14
SYSCONFDIR = /etc/postgresql-common
PGXS = /usr/lib/postgresql/14/lib/pgxs/src/makefiles/pgxs.mk
CONFIGURE =  '--build=x86_64-linux-gnu' '--prefix=/usr' '--includedir=${prefix}/include' '--mandir=${prefix}/share/man' '--infodir=${prefix}/share/info' '--sysconfdir=/etc' '--localstatedir=/var' '--disable-option-checking' '--disable-silent-rules' '--libdir=${prefix}/lib/x86_64-linux-gnu' '--runstatedir=/run' '--disable-maintainer-mode' '--disable-dependency-tracking' '--with-tcl' '--with-perl' '--with-python' '--with-pam' '--with-openssl' '--with-libxml' '--with-libxslt' '--mandir=/usr/share/postgresql/14/man' '--docdir=/usr/share/doc/postgresql-doc-14' '--sysconfdir=/etc/postgresql-common' '--datarootdir=/usr/share/' '--datadir=/usr/share/postgresql/14' '--bindir=/usr/lib/postgresql/14/bin' '--libdir=/usr/lib/x86_64-linux-gnu/' '--libexecdir=/usr/lib/postgresql/' '--includedir=/usr/include/postgresql/' '--with-extra-version= (Ubuntu 14.17-0ubuntu0.22.04.1)' '--enable-nls' '--enable-thread-safety' '--enable-debug' '--enable-dtrace' '--disable-rpath' '--with-uuid=e2fs' '--with-gnu-ld' '--with-gssapi' '--with-ldap' '--with-pgport=5432' '--with-system-tzdata=/usr/share/zoneinfo' 'AWK=mawk' 'MKDIR_P=/bin/mkdir -p' 'PROVE=/usr/bin/prove' 'PYTHON=/usr/bin/python3' 'TAR=/bin/tar' 'XSLTPROC=xsltproc --nonet' 'CFLAGS=-g -O2 -flto=auto -ffat-lto-objects -flto=auto -ffat-lto-objects -fstack-protector-strong -Wformat -Werror=format-security -fno-omit-frame-pointer' 'LDFLAGS=-Wl,-Bsymbolic-functions -flto=auto -ffat-lto-objects -flto=auto -Wl,-z,relro -Wl,-z,now' '--enable-tap-tests' '--with-icu' '--with-llvm' 'LLVM_CONFIG=/usr/bin/llvm-config-14' 'CLANG=/usr/bin/clang-14' '--with-lz4' '--with-systemd' '--with-selinux' 'build_alias=x86_64-linux-gnu' 'CPPFLAGS=-Wdate-time -D_FORTIFY_SOURCE=2' 'CXXFLAGS=-g -O2 -flto=auto -ffat-lto-objects -flto=auto -ffat-lto-objects -fstack-protector-strong -Wformat -Werror=format-security'
CC = gcc
CPPFLAGS = -Wdate-time -D_FORTIFY_SOURCE=2 -D_GNU_SOURCE -I/usr/include/libxml2
CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Werror=vla -Wendif-labels -Wmissing-format-attribute -Wimplicit-fallthrough=3 -Wcast-function-type -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -Wno-format-truncation -Wno-stringop-truncation -g -g -O2 -flto=auto -ffat-lto-objects -flto=auto -ffat-lto-objects -fstack-protector-strong -Wformat -Werror=format-security -fno-omit-frame-pointer
CFLAGS_SL = -fPIC
LDFLAGS = -Wl,-Bsymbolic-functions -flto=auto -ffat-lto-objects -flto=auto -Wl,-z,relro -Wl,-z,now -L/usr/lib/llvm-14/lib -Wl,--as-needed
LDFLAGS_EX = 
LDFLAGS_SL = 
LIBS = -lpgcommon -lpgport -lselinux -llz4 -lxslt -lxml2 -lpam -lssl -lcrypto -lgssapi_krb5 -lz -lreadline -lm 
VERSION = PostgreSQL 14.17 (Ubuntu 14.17-0ubuntu0.22.04.1) command-line utility to provide information about PostgreSQL installation directories, compiler options, and build configuration
- The  function creates an array of 23 ConfigData entries covering all major configuration aspects including directory paths (BINDIR, LIBDIR, etc.), build tools (CC, CFLAGS, etc.), and version information