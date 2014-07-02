Remote Ivory Repository
=======================

Overview
--------

Key idea is that an Ivory repo can refer to the fact sets and dictionaries in one or more *remote* repository. Both
repositories would need to be on the same filesystem.


Motivation
----------

Remote repos would allow the use cases such as the following to be supported:

#### Ephemeral fact sets

There may be scenarios where there are constraints on the length of time a fact sets can exist. For example, it may
be prohibitively expensive due to its size for a set of facts to be generated on a daily basis. During the period that
they are alive, however, there is a desire to perform a queries on a feature store containing the *ephemeral fact sets*
as well as all others. In that sense, you could refer to such a feature store as an *ephemeral feature store*.

This can be achieved by maintaining two Ivory repositories: a *master* repository and an *ephemeral* repository. The
master repository is the main repository containing all main fact sets. The ephemeral repository contains fact sets that
are ephemeral as well as references to fact sets in the *master* repository.


#### Sandboxes

repos for individuals to cook up their own features:


#### Delta-based snapshotting

* improve performance of snapshots as of "now"
* steps:
  1. generate a snapshot using the traditional approach
  2. store the snapshot as a fact set in another repo, the "snapshot" repo
  3. in the "snapshot repo" create a feature store that includes the snapshot fact set and all fact sets
  added since the first snapshot
  4. the next snapshot as of "now" is performed on this feature store, which already has the majority of
  the fact history rolled up


Implementation
--------------

Basic thoughts:

* Each repo can define multiple *remote* repos. This is simply an identifier and a reference to the location of the
other repo (i.e. path).
* Feature stores can specify:
  * *local* fact sets, i.e. fact sets within the same repo as the store
  * *remote* facts sets, denoted by the remote identifier and the fact set identifier
* Something similar should be supported for dictionaries too


Generalising the idea of versioning
-----------------------------------

One of the core ideas of Ivory is that it is an immutable *database* of facts. Immutable views or *versions* of
the database are constructed by combining a specific feature store and dictionary together. All queries, then,
should be with respect to a particular *version*. Whilst the design of ivory allows for the notion of versions,
it is currently not a first class citizen. Furthermore, it were to be made a first class citizen, the mechanism
for dealing with remote repos may fall out more naturally.

There are a number of *objects* in our data model that should be versioned:

* dictionaries
* feature stores
* fact sets
* repositories - i.e. a dictionary-store pair

It may be worth borrowing ideas from Git on how this is designed. For example:

* Version identifiers are hashes of their content. For fact sets we could use CRCs associated with the data.
* Have human-readable references to identifiers, i.e. *branches* and *tags*.
* The concept of branches is interesting in that it suggests a lineage between different versions. Given the
changes to dictionaries and feature store are typically incremental in nature, the idea of a version being
a delta applied to a *parent* version may be worth while.
* This all, of course, plays in to the *remote* concept. That is, remote fact sets can be referenced by version.
