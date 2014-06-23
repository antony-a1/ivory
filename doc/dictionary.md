The Ivory Dictionary
====================

### Context

Related work:

 - This assumes we are moving towards <https://github.com/ambiata/ivory/blob/master/doc/content-store.md>.
 - This is a part of larger piece of work highlighted by <https://github.com/ambiata/ivory/blob/master/doc/feature-gen.md> so decisions should be made in light of that.

Related issues:

 - <https://github.com/ambiata/ivory/issues/57>
 - <https://github.com/ambiata/ivory/issues/56>
 - <https://github.com/ambiata/ivory/issues/44>
 - <https://github.com/ambiata/ivory/issues/22>

### Overview

The dictionary is a critical component of ivory. It serves as a basis
for data validation and integrity, and the intention is it will be
extended to serve as the basis for feature generation. But to meet these
goals we need address a number of issues:
 - The dictionary must be easily transportable, we need to send it to
   map-reduce jobs, import and export easily.
 - It must be relatively space efficient, whilst it is not huge, looking
   at a scale maxing out at 10,000s of features, it contains a lot of
   clumsy data like feature names and descriptions as strings.
 - It must be evolvable. We will often need to update the dictionary schema
   to support new ivory features.

This work is to move the current dictionary support in ivory to support these
goals.


### Current State

Currently dictionaries are stored internally in a psv text format:

```
<namespace>|<feature>|<encoding>|<type>|<description>|<tombstones (comma sep)>
```

This representations acts as both the input format and the output
format for dictionaries. All transport is done in this format.

The implementation is here:
 - <https://github.com/ambiata/ivory/blob/master/ivory-storage/src/main/scala/com/ambiata/ivory/storage/legacy/DictionaryTextStorage.scala>
 - <https://github.com/ambiata/ivory/blob/master/ivory-ingest/src/main/scala/com/ambiata/ivory/ingest/DictionaryImporter.scala>

The current dictionary import workflow is very bespoke, it happens in two
phases, a "named" dictionary is imported explicitly.

<https://github.com/ambiata/ivory/blob/master/ivory-ingest/src/main/scala/com/ambiata/ivory/ingest/DictionaryImporter.scala#L19-L27>

It gets dumped in `<repository>/metadata/dictionaries/<name>` to be read later.

Then on each ingest/day, a copy of that dictionary is taken:

<https://github.com/ambiata/ivory/blob/master/ivory-storage/src/main/scala/com/ambiata/ivory/storage/legacy/fatrepo/ImportWorkflow.scala#L99-L118>

This process create a copy of the latest "named" dictionary and assigns it
as "today's" dictionary in `<repository>/metadata/dictionaries/<yyyy-MM-dd>`.
It is today's dictionary that is used for standard ivory ingestion and
extractions processes.

Observations on the current process:
 - The format is not extensible, the fixed number of columns is not capable
   of dealing with optional values, or with disjoint sets of attributes for
   different types of features.

 - The format is very heavy, storing as text makes it expensive to re-parse
   during MR jobs.

 - Dictionaries should not have "names", there should be a dictionary
   associated with the repository, and that is it.

 - The workflow around dictionaries is really complicated and clunky.

 - The workflow process is _broken_ because you potentially lose
   intermediate dictionaries if you do two named imports on the same
   day or if you don't ingest every day.

 - There is no way to evolve the dictionary in a sane way.

 - There is no (elegant or correct) way to remove an entry from the dictionary.


### Work

1. Build out an ivory repository config file format. The format needs to
   include:
   - The meta-data version (i.e. the schema version not the versions of values stored).
     Default to `1` for existing repositories without a version, which we will say
     is the current text versions spec'd out above. And Default to `<latest>` for
     new repositories.

   - The repository timezone (This will be tricky to migrate to, but given that we
     know the only users are in "Australia/Sydney" that will have to be the default,
     and we will need to mandate a timezone id on `ivory create-repo` in the future).
     The motivation for this is discussed in <https://github.com/ambiata/ivory/blob/master/doc/dates.md>.

   The config should map to a data-structure in `ivory-core`, with standard mechanisms
   to print an parse.

   Add a path onto `Repository` that specifies location of config in
   repository, probably `metadata/ivory.conf` or similar.

   The config is probably best as plain text as it is useful to be able to
   inspect it without ivory tooling.


2. Build up an ivory repository start-up module that controls reading
   the current config and allows for auto-updating as required.

   All apps that make updates to ivory (pretty much everything except `cat-*`)
   should go through this routine of checking the current repository state,
   and updating as required.

   The auto-update is the most critical part of this. We need to perform
   it in such a way that multiple people running ivory don't crash into
   each other (this can be quite difficult given the lack of decent
   atomicity / locking constructs) but ivory makes this slightly easier
   in that we only add new files, never update existing ones outside of
   these migrations.


3. Create new formats for dictionaries.

   An internal representation the is a well-typed thrift struct
   representing the current fields we support.

   A new text-based import and export format, that has better
   support for extensibility.

   We might want to get the data-scientists involved in the format
   conversation just to make sure we don't do anything that will
   make it hard to read in any of their tooling, but we will need
   to continue supporting the old format for a while, so this doesn't
   need to be finalized straight away.

   The text format will also need a way to specify that an entry has
   been removed (which is done as a side-effect of the file clobbering
   and only importing whole dictionaries at the moment).

   For example, something like this might work for text format:
  `demographics:age|encoding=int|type=continuous|tombstone=NA|description=the stone age`


4. Update ingestion workflow.

   Ingestion should just always use the "latest" version of the dictionary.

   Ingestion should _not_ take a copy the dictionary.


5. Update import dictionary.

   Add support for the new ingestion format (while keeping the old one as well).

   Remove the named dictionary concept.

   Always import into the internal thrift format.

   Support update vs override. i.e. should the imported dictionary totally
   supersede the current one, or should it be merged with the current one.
