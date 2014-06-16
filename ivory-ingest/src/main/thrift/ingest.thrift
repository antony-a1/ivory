namespace java com.ambiata.ivory.ingest

struct NamespaceLookup {
    1: map<i32, string> namespaces;
}

struct ReducerLookup {
    1: map<i32, i32> reducers;
}

struct FeatureIdLookup {
    1: map<string, i32> ids;
}
