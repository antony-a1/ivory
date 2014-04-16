scalacOptions += "-deprecation"

resolvers += "Era7 maven releases" at "http://releases.era7.com.s3.amazonaws.com"

resolvers += Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.ambiata" % "promulgate" % "0.11.0-20140410040932-7b5bc30")
