scalacOptions += "-deprecation"

resolvers += Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.ambiata" % "promulgate" % "0.11.0-20140410040932-7b5bc30")
