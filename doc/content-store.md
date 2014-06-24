ivory as a generalized meta-data store
======================================

At the core of ivory is the ability to store data sets and meta-data
about those data sets. This spec is largely concerned with describing
the core versioned k-v store used as the basis for storing meta-data.

meta-data vs data terminology
-----------------------------

  - `data`: The actual data we are describing, in a feature store
    these are facts.

  - `meta-data`: The description of data, in a feature store this is
     currently the dictionary, and links to the fact-sets (i.e. the
     fact store).

This distinction is somewhat superficial, but the important line is
that data is too large to store/transfer/manage in a generalized way,
however its description and management (i.e. its meta-data) can be
easily sync'd and manipulated without custom tooling.

the meta-data store
-------------------

The meta-data store allows for a series of versions of a key-value
store. Each key is versioned individually.

### versions

Versions are a monotonically increasing 32 bit number rendered in hex:
 - `00000000` through to `ffffffff`

### keys

Keys are simply convenient identifiers for referring to values. All
keys must consist of only alpha-numeric characters or hyphens '-' if a
separator is required. This (enforced) convention is somewhat
arbitrary, but forces a level of consistency and will also allow for
implementations which have restrictions on filenames or values.

### values

Values are semi-structured data, each value should have a defined
format and should be _eagerly_ updated (i.e. there is a single
descriptor describing the state of all meta-data in the repository,
and any update should first update the meta-data to the latest
version).

Each value may have it's own format, but the preference is to to tend
towards compact-encoded thrift structures.

### storage

The store should support local disk (posix), s3 and hdfs. For an
initial implementation, it is proposed that we use a consistent
on-disk representation for posix, s3 & hdfs.

```
<repository>
  /metadata
    /{key}
      /{version}
        /{value}
```

We should support repositories that work with metadata/data stored
on a different storage mechanism. The following should be supported:

|metadata storage  |data storage |
|------------------|-------------|
|memory            |memory       |
|memory            |posix        |
|posix             |posix        |
|posix             |hdfs         |
|posix             |s3           |
|s3                |s3           |
|hdfs              |hdfs         |


### implementation details

#### atomic operations (HDFS edition)

To avoid data races, or losing data, we will have the need for atomic
operations. Atomic operations may be performed in 1 of 2 ways:
 - Atomic move (via hadoop 2+ FileContext api) [preferred]
 - Explicit locking (via name-node or some other centralized mechanism)

Support for atomic operations should be a core tool within ivory.
*Implementation detail to be added*.

T.B.D. Discuss with Russell who has an implementation using atomic
moves.

T.B.D. A "staging" like area followed by a commit probably makes a
sense.

### feature store specifics

#### Dictionaries

The ivory dictionary.

The dictionary key is `dictionary`.

The dictionary value is a thrift struct defined in `ivory-core`.

#### Stores

Stores lists of fact-sets that contribute to the current "store".

The store key is `store`.

The store value is a text value defined in `ivory-core`.

#### Manifest

Used for repository integrity checks.

The store key is `manifest`.

The store value is a thrift structure defined in `ivory-core`.

#### Repository

Used as a reference node for versioning multiple values at a time.
Contains a list of all the other key/values.

The store key is `repository`.

The store value is a text value defined in `ivory-core`.


## Justification / Discussion

justification
-------------

For brevity, the above attempts to concentrate on the final design for
the meta-data store inside of ivory. However, for context it is
probably useful to document some of the reasons why this is so.

Ivory, as is currently stands, is a number of low-level commands to
perform specific (as in they know about the individual concepts)
operations on repositories, dictionaries, fact stores, and fact sets,
coupled with a set of high-level conventions for using those tools
together as a "Feature Store". However, the future requirements for
ivory will be quite challenging and continuing in this bespoke manner
where there a large number of specific tools is likely to be painfully
difficult to extend and worse we will end up with weird
inconsistencies between each of the individual tools. This specs out a
solution for this by introducing a uniform underlying system for
storing data and meta-data, and then allowing us to build the specific
concepts (hopefully easily) in terms of this underlying system. From
this solution, we should also see implementations of versioning of
meta-data, remotes (aggregated repositories) and labeling fall out
naturally.

### properties of meta-data

These restrictions may not be necessary, but it is likely they will
help pull out some general properties, rather than the 'everything is
a string and the burden is on the client to parse' philosophy.

 - meta-data can always be appended to but the addition is not
   necessarily commutative (e.g. add fact sets to store, add
   entries to dictionary effects priority).

 - meta-data is categorized into different objects, as in there isn't
   just a giant blob of meta-data, we want to be able to label pieces
   for individual extraction / manipulation.

 - meta-data should be linear, as in it progresses from one state to
   the next, there is never multiple ancestors to a given state. From
   a machine driven perspective it is not a desirable property to be
   able to inspect or reason about multiple ancestors because it
   prevents us from mechanically and deterministic processing
   updates (where we don't force changes to be commutative).

### versions

There are really two options:
 1. monotonically increasing
 2. content addressed

The problem with content addressed in this specific situation is
really limited by implementation options in that we don't have any
decent hash implementations on the JVM, and shelling out or
implementing one is just not going to work at the
moment. Specifically what I mean by "decent" is:

 - Parallelizable, as in we would want something that does tree-hashing.
   Being forced to process things sequentially just because of your
   versioning scheme is daft and will likely run into a wall at some
   point.

 - Incremental, as in we would not want to reprocess existing data
   unless absolutely necessary, so we would want to be able serialize
   hashing state attached to each version so we only need to hash
   the deltas at each stage.

Neither of these things are likely to happen. So we are left with
monotonically increasing. This gives us a far simpler model (and given
that we don't really want to store loose objects if we have HDFS
underneath us due to block size constraints we don't really lose
anything that CAS give you anyway. For aesthetics and consistency
we can go with monotonically increasing 32 bit number rendered in hex,
So `00000000` through to `ffffffff`.
