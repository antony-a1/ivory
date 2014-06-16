import sbt._
import Keys._

object depend {
  val scalaz    = Seq("org.scalaz"           %% "scalaz-core"     % "7.0.6",
                      "org.scalaz"           %% "scalaz-effect"   % "7.0.6")
  val scopt     = Seq("com.github.scopt"     %% "scopt"           % "3.2.0")
  val trove     = Seq("trove"                %  "trove"           % "1.0.2")
  val joda      = Seq("joda-time"            %  "joda-time"       % "2.1",
                      "org.joda"             %  "joda-convert"    % "1.1")
  val specs2    = Seq("org.specs2"           %% "specs2-core",
                      "org.specs2"           %% "specs2-junit",
                      "org.specs2"           %% "specs2-html",
                      "org.specs2"           %% "specs2-matcher-extra",
                      "org.specs2"           %% "specs2-scalacheck").map(_ % "2.3.10")
  val commonsio = Seq("commons-io"           %  "commons-io"      % "2.4")
  val rng       = Seq("com.nicta"            %% "rng"             % "1.2.1")
  val thrift    = Seq("org.apache.thrift"    %  "libthrift"       % "0.9.1")
  val saws      = Seq("com.ambiata"          %% "saws"            % "1.2.1-20140415232043-cc305d0",
                      "net.java.dev.jets3t"  %  "jets3t"          % "0.9.0" )
  val mundane   = Seq("com.ambiata"          %% "mundane"         % "1.2.1-20140505014353-a175c40")
  val caliper   = Seq("com.google.caliper"   %  "caliper"         % "0.5-rc1")

  def scoobi(version: String) = {
    val scoobiVersion =
      if (version.contains("cdh3"))      "0.9.0-cdh3-20140528074037-3adbe0e"
      else if (version.contains("cdh4")) "0.9.0-cdh4-20140528074037-3adbe0e"
      else if (version.contains("cdh5")) "0.9.0-cdh5-20140610021710-7b2296d"
      else                               "0.9.0-cdh5-20140610021710-7b2296d"


    Seq("com.nicta" %% "scoobi" % scoobiVersion excludeAll(ExclusionRule(organization = "org.apache.hadoop"))) ++ hadoop(version)
  }

  def hadoop(version: String, hadoopVersion: String = "2.2.0") =
    if (version.contains("cdh3"))      Seq("org.apache.hadoop" % "hadoop-core"   % "0.20.2-cdh3u1" % "provided",
                                           "org.apache.avro"   % "avro-mapred"   % "1.7.4" % "provided")

    else if (version.contains("cdh4")) Seq("org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.6.0" % "provided" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-core"   % "2.0.0-mr1-cdh4.6.0" % "provided",
                                           "org.apache.avro"   % "avro-mapred"   % "1.7.4" % "provided" classifier "hadoop2")

    else if (version.contains("cdh5")) Seq("org.apache.hadoop" % "hadoop-client" % "2.2.0-cdh5.0.0-beta-2" % "provided" exclude("asm", "asm"),
                                           "org.apache.avro"   % "avro-mapred"   % "1.7.5-cdh5.0.0-beta-2" % "provided")

    else                               Seq("org.apache.hadoop" % "hadoop-common"                     % hadoopVersion % "provided" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-hdfs"                       % hadoopVersion % "provided" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-mapreduce-client-app"       % hadoopVersion % "provided" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-mapreduce-client-core"      % hadoopVersion % "provided" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "provided" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-mapreduce-client-core"      % hadoopVersion % "provided" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-annotations"                % hadoopVersion % "provided" exclude("asm", "asm"),
                                           "org.apache.avro"   % "avro-mapred"                       % "1.7.4" % "provided" classifier "hadoop2")

  val resolvers = Seq(
      Resolver.sonatypeRepo("releases"),
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("public"),
      Resolver.typesafeRepo("releases"),
      "cloudera"             at "https://repository.cloudera.com/content/repositories/releases",
      Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns),
      "Scalaz Bintray Repo"  at "http://dl.bintray.com/scalaz/releases")
}
