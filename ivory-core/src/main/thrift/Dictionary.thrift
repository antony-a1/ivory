namespace java com.ambiata.ivory.core.thrift

enum ThriftDictionaryEncoding {
    BOOLEAN,
    INT,
    LONG,
    DOUBLE,
    STRING
}

enum ThriftDictionaryType {
    NUMERICAL,
    CONTINOUS,
    CATEGORICAL,
    BINARY
}

struct ThriftDictionaryFeatureId {
    1: string ns;
    2: string name;
}

struct ThriftDictionaryFeatureMeta {
    1: ThriftDictionaryEncoding encoding;
    2: ThriftDictionaryType type;
    3: string desc;
    4: list<string> tombstoneValue;
}

struct ThriftDictionary {
    1: map<ThriftDictionaryFeatureId, ThriftDictionaryFeatureMeta> meta;
}
