# hash_create

## Location
[src/backend/utils/hash/dynahash.c:352-629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L352-L629)

## Overview
hash_create is the primary function for creating new dynamic hash tables in PostgreSQL, providing a comprehensive interface for configuring hash table parameters and behavior.

## Definition

```c
struct) that
	 * we allocate in TopMemoryContext;
```
## Detailed Description
hash_create serves as the main entry point for creating dynamic hash tables in PostgreSQL's hash table infrastructure. This function supports extensive customization through flags and the HASHCTL structure, allowing callers to specify hash functions, comparison functions, memory contexts, key copying methods, and various optimization settings. The function handles both shared-memory and private hash tables, with different allocation strategies for each. It performs comprehensive validation of input parameters, sets up appropriate defaults based on key type (strings, blobs, or custom), and initializes the complete hash table structure including directory segments and freelists.

## Parameters / Member Variables
- : Name for the hash table (used for debugging and memory context identification)
- : Maximum expected number of elements (used for initial sizing)
- File: dir,	Node: Top,	This is the top of the INFO tree.

This is the Info main menu (aka directory node).
A few useful Info commands:

  'q' quits;
  'H' lists all Info commands;
  'h' starts the Info tutorial;
  'mTexinfo RET' visits the Texinfo manual, etc.

* Menu:

Basics
* Common options: (coreutils)Common options.
* Coreutils: (coreutils).       Core GNU (file, text, shell) utilities.
* Date input formats: (coreutils)Date input formats.
* Ed: (ed).                     The GNU line editor
* File permissions: (coreutils)File permissions.
                                Access modes.
* Finding files: (find).        Operating on files matching certain criteria.
* Time: (time).                 time

C++ libraries
* autosprintf: (autosprintf).   Support for printf format strings in C++.

Compression
* Gzip: (gzip).                 General (de)compression of files (lzw).
* Lzip: (lzip).                 LZMA lossless data compressor

Development
* Global: (global).             GNU Global source code tagging system.

DOS
* Mtools: (mtools).             Mtools: utilities to access DOS disks in Unix.

Editors
* nano: (nano).                 Small and friendly text editor.

Emacs
* Emacs FAQ: (efaq).            Frequently Asked Questions about Emacs.

General Commands
* Screen: (screen).             Full-screen window manager.

GNU Gettext Utilities
* autopoint: (gettext)autopoint Invocation.
                                Copy gettext infrastructure.
* envsubst: (gettext)envsubst Invocation.
                                Expand environment variables.
* gettextize: (gettext)gettextize Invocation.
                                Prepare a package for gettext.
* gettext: (gettext).           GNU gettext utilities.
* ISO3166: (gettext)Country Codes.
                                ISO 3166 country codes.
* ISO639: (gettext)Language Codes.
                                ISO 639 language codes.
* msgattrib: (gettext)msgattrib Invocation.
                                Select part of a PO file.
* msgcat: (gettext)msgcat Invocation.
                                Combine several PO files.
* msgcmp: (gettext)msgcmp Invocation.
                                Compare a PO file and template.
* msgcomm: (gettext)msgcomm Invocation.
                                Match two PO files.
* msgconv: (gettext)msgconv Invocation.
                                Convert PO file to encoding.
* msgen: (gettext)msgen Invocation.
                                Create an English PO file.
* msgexec: (gettext)msgexec Invocation.
                                Process a PO file.
* msgfilter: (gettext)msgfilter Invocation.
                                Pipe a PO file through a filter.
* msgfmt: (gettext)msgfmt Invocation.
                                Make MO files out of PO files.
* msggrep: (gettext)msggrep Invocation.
                                Select part of a PO file.
* msginit: (gettext)msginit Invocation.
                                Create a fresh PO file.
* msgmerge: (gettext)msgmerge Invocation.
                                Update a PO file from template.
* msgunfmt: (gettext)msgunfmt Invocation.
                                Uncompile MO file into PO file.
* msguniq: (gettext)msguniq Invocation.
                                Unify duplicates for PO file.
* ngettext: (gettext)ngettext Invocation.
                                Translate a message with plural.
* xgettext: (gettext)xgettext Invocation.
                                Extract strings into a PO file.

GNU organization
* Maintaining Findutils: (find-maint).
                                Maintaining GNU findutils

GNU Utilities
* dirmngr-client: (gnupg).      X.509 CRL and OCSP client.
* dirmngr: (gnupg).             X.509 CRL and OCSP server.
* gpg-agent: (gnupg).           The secret key daemon.
* gpg2: (gnupg).                OpenPGP encryption and signing tool.
* gpgsm: (gnupg).               S/MIME encryption and signing tool.

Individual utilities
* aclocal-invocation: (automake-1.16)aclocal Invocation.
                                                Generating aclocal.m4.
* arch: (coreutils)arch invocation.             Print machine hardware name.
* automake-invocation: (automake-1.16)automake Invocation.
                                                Generating Makefile.in.
* b2sum: (coreutils)b2sum invocation.           Print or check BLAKE2 digests.
* base32: (coreutils)base32 invocation.         Base32 encode/decode data.
* base64: (coreutils)base64 invocation.         Base64 encode/decode data.
* basename: (coreutils)basename invocation.     Strip directory and suffix.
* basenc: (coreutils)basenc invocation.         Encoding/decoding of data.
* cat: (coreutils)cat invocation.               Concatenate and write files.
* chcon: (coreutils)chcon invocation.           Change SELinux CTX of files.
* chgrp: (coreutils)chgrp invocation.           Change file groups.
* chmod: (coreutils)chmod invocation.           Change access permissions.
* chown: (coreutils)chown invocation.           Change file owners and groups.
* chroot: (coreutils)chroot invocation.         Specify the root directory.
* cksum: (coreutils)cksum invocation.           Print POSIX CRC checksum.
* cmp: (diffutils)Invoking cmp.                 Compare 2 files byte by byte.
* comm: (coreutils)comm invocation.             Compare sorted files by line.
* cp: (coreutils)cp invocation.                 Copy files.
* csplit: (coreutils)csplit invocation.         Split by context.
* cut: (coreutils)cut invocation.               Print selected parts of lines.
* date: (coreutils)date invocation.             Print/set system date and time.
* dd: (coreutils)dd invocation.                 Copy and convert a file.
* df: (coreutils)df invocation.                 Report file system disk usage.
* diff3: (diffutils)Invoking diff3.             Compare 3 files line by line.
* diff: (diffutils)Invoking diff.               Compare 2 files line by line.
* dir: (coreutils)dir invocation.               List directories briefly.
* dircolors: (coreutils)dircolors invocation.   Color setup for ls.
* dirname: (coreutils)dirname invocation.       Strip last file name component.
* du: (coreutils)du invocation.                 Report on disk usage.
* echo: (coreutils)echo invocation.             Print a line of text.
* env: (coreutils)env invocation.               Modify the environment.
* expand: (coreutils)expand invocation.         Convert tabs to spaces.
* expr: (coreutils)expr invocation.             Evaluate expressions.
* factor: (coreutils)factor invocation.         Print prime factors
* false: (coreutils)false invocation.           Do nothing, unsuccessfully.
* find: (find)Invoking find.                    Finding and acting on files.
* fmt: (coreutils)fmt invocation.               Reformat paragraph text.
* fold: (coreutils)fold invocation.             Wrap long input lines.
* groups: (coreutils)groups invocation.         Print group names a user is in.
* gunzip: (gzip)Overview.                       Decompression.
* gzexe: (gzip)Overview.                        Compress executables.
* head: (coreutils)head invocation.             Output the first part of files.
* hostid: (coreutils)hostid invocation.         Print numeric host identifier.
* hostname: (coreutils)hostname invocation.     Print or set system name.
* id: (coreutils)id invocation.                 Print user identity.
* install: (coreutils)install invocation.       Copy files and set attributes.
* join: (coreutils)join invocation.             Join lines on a common field.
* kill: (coreutils)kill invocation.             Send a signal to processes.
* link: (coreutils)link invocation.             Make hard links between files.
* ln: (coreutils)ln invocation.                 Make links between files.
* locate: (find)Invoking locate.                Finding files in a database.
* logname: (coreutils)logname invocation.       Print current login name.
* ls: (coreutils)ls invocation.                 List directory contents.
* md5sum: (coreutils)md5sum invocation.         Print or check MD5 digests.
* mkdir: (coreutils)mkdir invocation.           Create directories.
* mkfifo: (coreutils)mkfifo invocation.         Create FIFOs (named pipes).
* mknod: (coreutils)mknod invocation.           Create special files.
* mktemp: (coreutils)mktemp invocation.         Create temporary files.
* mv: (coreutils)mv invocation.                 Rename files.
* nice: (coreutils)nice invocation.             Modify niceness.
* nl: (coreutils)nl invocation.                 Number lines and write files.
* nohup: (coreutils)nohup invocation.           Immunize to hangups.
* nproc: (coreutils)nproc invocation.           Print the number of processors.
* numfmt: (coreutils)numfmt invocation.         Reformat numbers.
* od: (coreutils)od invocation.                 Dump files in octal, etc.
* paste: (coreutils)paste invocation.           Merge lines of files.
* patch: (diffutils)Invoking patch.             Apply a patch to a file.
* pathchk: (coreutils)pathchk invocation.       Check file name portability.
* pr: (coreutils)pr invocation.                 Paginate or columnate files.
* printenv: (coreutils)printenv invocation.     Print environment variables.
* printf: (coreutils)printf invocation.         Format and print data.
* ptx: (coreutils)ptx invocation.               Produce permuted indexes.
* pwd: (coreutils)pwd invocation.               Print working directory.
* readlink: (coreutils)readlink invocation.     Print referent of a symlink.
* realpath: (coreutils)realpath invocation.     Print resolved file names.
* rm: (coreutils)rm invocation.                 Remove files.
* rmdir: (coreutils)rmdir invocation.           Remove empty directories.
* runcon: (coreutils)runcon invocation.         Run in specified SELinux CTX.
* sdiff: (diffutils)Invoking sdiff.             Merge 2 files side-by-side.
* seq: (coreutils)seq invocation.               Print numeric sequences
* sha1sum: (coreutils)sha1sum invocation.       Print or check SHA-1 digests.
* sha2: (coreutils)sha2 utilities.              Print or check SHA-2 digests.
* shred: (coreutils)shred invocation.           Remove files more securely.
* shuf: (coreutils)shuf invocation.             Shuffling text files.
* sleep: (coreutils)sleep invocation.           Delay for a specified time.
* sort: (coreutils)sort invocation.             Sort text files.
* split: (coreutils)split invocation.           Split into pieces.
* stat: (coreutils)stat invocation.             Report file(system) status.
* stdbuf: (coreutils)stdbuf invocation.         Modify stdio buffering.
* stty: (coreutils)stty invocation.             Print/change terminal settings.
* sum: (coreutils)sum invocation.               Print traditional checksum.
* sync: (coreutils)sync invocation.             Synchronize memory to disk.
* tac: (coreutils)tac invocation.               Reverse files.
* tail: (coreutils)tail invocation.             Output the last part of files.
* tee: (coreutils)tee invocation.               Redirect to multiple files.
* test: (coreutils)test invocation.             File/string tests.
* timeout: (coreutils)timeout invocation.       Run with time limit.
* touch: (coreutils)touch invocation.           Change file timestamps.
* tr: (coreutils)tr invocation.                 Translate characters.
* true: (coreutils)true invocation.             Do nothing, successfully.
* truncate: (coreutils)truncate invocation.     Shrink/extend size of a file.
* tsort: (coreutils)tsort invocation.           Topological sort.
* tty: (coreutils)tty invocation.               Print terminal name.
* uname: (coreutils)uname invocation.           Print system information.
* unexpand: (coreutils)unexpand invocation.     Convert spaces to tabs.
* uniq: (coreutils)uniq invocation.             Uniquify files.
* unlink: (coreutils)unlink invocation.         Removal via unlink(2).
* updatedb: (find)Invoking updatedb.            Building the locate database.
* uptime: (coreutils)uptime invocation.         Print uptime and load.
* users: (coreutils)users invocation.           Print current user names.
* vdir: (coreutils)vdir invocation.             List directories verbosely.
* wc: (coreutils)wc invocation.                 Line, word, and byte counts.
* wdiff: (wdiff)wdiff invocation.               Word difference finder.
* who: (coreutils)who invocation.               Print who is logged in.
* whoami: (coreutils)whoami invocation.         Print effective user ID.
* xargs: (find)Invoking xargs.                  Operating on many files.
* yes: (coreutils)yes invocation.               Print a string indefinitely.
* zcat: (gzip)Overview.                         Decompression to stdout.
* zdiff: (gzip)Overview.                        Compare compressed files.
* zforce: (gzip)Overview.                       Force .gz extension on files.
* zgrep: (gzip)Overview.                        Search compressed files.
* zmore: (gzip)Overview.                        Decompression output by pages.

Libraries
* RLuserman: (rluserman).       The GNU readline library User's Manual.

Math
* bc: (bc).                     An arbitrary precision calculator language.

Network applications
* Wget: (wget).                 Non-interactive network downloader.

Programming
* flex: (flex).                 Fast lexical analyzer generator (lex 
                                  replacement).

Software development
* Automake: (automake-1.16).    Making GNU standards-compliant Makefiles.
* Automake-history: (automake-history).
                                History of Automake development.

Texinfo documentation system
* Texinfo: (texinfo).           The GNU documentation format.
* info stand-alone: (info-stnd).
                                Read Info documents without Emacs.
* install-info: (texinfo)Invoking install-info.
                                Update info/dir entries.
* makeinfo: (texinfo)Invoking makeinfo.
                                Translate Texinfo source.
* pdftexi2dvi: (texinfo)PDF Output.
                                PDF output for Texinfo.
* pod2texi: (pod2texi)Invoking pod2texi.
                                Translate Perl POD to Texinfo.
* texi2dvi: (texinfo)Format with texi2dvi.
                                Print Texinfo documents.
* texi2pdf: (texinfo)PDF Output.
                                PDF output for Texinfo.
* texindex: (texinfo)Format with tex/texindex.
                                Sort Texinfo index files.

Text creation and manipulation
* Diffutils: (diffutils).       Comparing and merging files.
* M4: (m4).                     A powerful macro processor.
* Word differences: (wdiff).    GNU wdiff and diff related tools.
* grep: (grep).                 Print lines that match patterns.
* sed: (sed).                   Stream EDitor.  : Pointer to HASHCTL structure containing additional configuration parameters
- : Bitmask specifying which parameters in info to use and table characteristics

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [MemoryContextSetIdentifier](../M/MemoryContextSetIdentifier.md)
  - [string_hash](../s/string_hash.md)
  - [string_compare](../s/string_compare.md)
  - [uint32_hash](../u/uint32_hash.md)
  - [tag_hash](../t/tag_hash.md)
  - [strlcpy](../s/strlcpy.md)
  - [DynaHashAlloc](../D/DynaHashAlloc.md)
  - [hdefault](hdefault.md)
  - [next_pow2_int](../n/next_pow2_int.md)
  - [my_log2](../m/my_log2.md)
  - [init_htab](../i/init_htab.md)
  - [element_alloc](../e/element_alloc.md)
- Called from (representative examples):
  - [InitBufferPoolAccess](../I/InitBufferPoolAccess.md)
  - [RelationCacheInitialize](../R/RelationCacheInitialize.md)  
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [InitLocks](../I/InitLocks.md)
  - [ReorderBufferAllocate](../R/ReorderBufferAllocate.md)

## Notes and Other Information
- HASH_ELEM flag is mandatory - must specify key and entry sizes
- Must specify exactly one of HASH_STRINGS, HASH_BLOBS, or HASH_FUNCTION
- HASH_CONTEXT allows using custom memory context instead of TopMemoryContext
- Supports both shared-memory and private hash tables with different allocation strategies
- Automatically selects appropriate hash and comparison functions based on key type
- Handles pre-allocation of elements for performance optimization
- Returns NULL on allocation failure when using MCXT_ALLOC_NO_OOM
- Located at src/backend/utils/hash/dynahash.c:352-629

## Simplified Source

```c
// Simplified version of hash_create
HTAB *hash_create(const char *tabname, long nelem, const HASHCTL *info, int flags) {
    HTAB *hashp;
    HASHHDR *hctl;

    // Validate required parameters
    Assert(flags & HASH_ELEM);
    Assert(info->keysize > 0);
    Assert(info->entrysize >= info->keysize);

    // Set up memory context - shared vs private
    if (flags & HASH_SHARED_MEM) {
        CurrentDynaHashCxt = TopMemoryContext;
    } else {
        // Create private memory context for hash table
        if (flags & HASH_CONTEXT) {
            CurrentDynaHashCxt = info->hcxt;
        } else {
            CurrentDynaHashCxt = TopMemoryContext;
        }
        CurrentDynaHashCxt = AllocSetContextCreate(CurrentDynaHashCxt,
                                                   "dynahash",
                                                   ALLOCSET_DEFAULT_SIZES);
    }

    // Allocate and initialize hash table header
    hashp = (HTAB *) MemoryContextAlloc(CurrentDynaHashCxt,
                                        sizeof(HTAB) + strlen(tabname) + 1);
    MemSet(hashp, 0, sizeof(HTAB));
    hashp->tabname = (char *) (hashp + 1);
    strcpy(hashp->tabname, tabname);

    // Select hash function based on key type
    if (flags & HASH_FUNCTION) {
        hashp->hash = info->hash;
    } else if (flags & HASH_BLOBS) {
        // Optimize for common key sizes
        if (info->keysize == sizeof(uint32)) {
            hashp->hash = uint32_hash;
        } else {
            hashp->hash = tag_hash;
        }
    } else {
        // String hashing (HASH_STRINGS)
        Assert(flags & HASH_STRINGS);
        hashp->hash = string_hash;
    }

    // Set comparison function
    if (flags & HASH_COMPARE) {
        hashp->match = info->match;
    } else if (hashp->hash == string_hash) {
        hashp->match = (HashCompareFunc) string_compare;
    } else {
        hashp->match = memcmp;
    }

    // Set key copying function
    if (flags & HASH_KEYCOPY) {
        hashp->keycopy = info->keycopy;
    } else if (hashp->hash == string_hash) {
        hashp->keycopy = (HashCopyFunc) (pg_funcptr_t) strlcpy;
    } else {
        hashp->keycopy = memcpy;
    }

    // Set allocation function
    if (flags & HASH_ALLOC) {
        hashp->alloc = info->alloc;
    } else {
        hashp->alloc = DynaHashAlloc;
    }

    // Handle shared memory setup
    if (flags & HASH_SHARED_MEM) {
        hashp->hctl = info->hctl;
        hashp->dir = (HASHSEGMENT *) (((char *) info->hctl) + sizeof(HASHHDR));
        hashp->isshared = true;

        // If attaching to existing table, copy parameters and return
        if (flags & HASH_ATTACH) {
            hctl = hashp->hctl;
            hashp->keysize = hctl->keysize;
            hashp->ssize = hctl->ssize;
            hashp->sshift = hctl->sshift;
            return hashp;
        }
    } else {
        // Private hash table setup
        hashp->hctl = NULL;
        hashp->dir = NULL;
        hashp->hcxt = CurrentDynaHashCxt;
        hashp->isshared = false;
    }

    // Allocate header control structure if needed
    if (!hashp->hctl) {
        hashp->hctl = (HASHHDR *) hashp->alloc(sizeof(HASHHDR));
        if (!hashp->hctl) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                           errmsg("out of memory")));
        }
    }

    // Initialize hash table defaults and configure parameters
    hashp->frozen = false;
    hdefault(hashp);
    hctl = hashp->hctl;

    // Configure partitioning, segments, and directory size
    if (flags & HASH_PARTITION) {
        hctl->num_partitions = info->num_partitions;
    }
    if (flags & HASH_SEGMENT) {
        hctl->ssize = info->ssize;
        hctl->sshift = my_log2(info->ssize);
    }
    if (flags & HASH_DIRSIZE) {
        hctl->max_dsize = info->max_dsize;
        hctl->dsize = info->dsize;
    }

    // Store entry parameters
    hctl->keysize = info->keysize;
    hctl->entrysize = info->entrysize;
    hashp->keysize = hctl->keysize;
    hashp->ssize = hctl->ssize;
    hashp->sshift = hctl->sshift;

    // Build hash directory structure
    if (!init_htab(hashp, nelem)) {
        elog(ERROR, "failed to initialize hash table \"%s\"", hashp->tabname);
    }

    // Pre-allocate elements if needed
    if ((flags & HASH_SHARED_MEM) || nelem < hctl->nelem_alloc) {
        // Calculate allocation distribution across freelists
        int freelist_partitions = IS_PARTITIONED(hashp->hctl) ? NUM_FREELISTS : 1;
        int nelem_alloc = nelem / freelist_partitions;
        if (nelem_alloc <= 0) nelem_alloc = 1;

        // Allocate elements for each freelist
        for (int i = 0; i < freelist_partitions; i++) {
            int allocation_size = (i == 0) ?
                nelem - nelem_alloc * (freelist_partitions - 1) : nelem_alloc;

            if (!element_alloc(hashp, allocation_size, i)) {
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                               errmsg("out of memory")));
            }
        }
    }

    // Set fixed size flag if specified
    if (flags & HASH_FIXED_SIZE) {
        hashp->isfixed = true;
    }

    return hashp;
}
```

Key simplifications made:
- Removed detailed comments for clarity while preserving essential logic
- Consolidated parameter validation into clear assertions
- Simplified memory context setup logic flow
- Streamlined hash function selection with clear branching
- Abstracted complex freelist allocation logic into simpler loops
- Focused on main execution path while preserving all core functionality
- Maintained error handling for critical allocation failures
- Preserved all essential configuration and initialization steps