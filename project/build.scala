import sbt._
import Keys._
import sbt.KeyRanks._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.ambiata.promulgate.project.ProjectPlugin._

object build extends Build {
  type Settings = Def.Setting[_]

  lazy val ivory = Project(
    id = "ivory"
  , base = file(".")
  , settings = standardSettings ++ promulgate.library(s"com.ambiata.ivory", "ambiata-oss")
  /* this should aggregate _all_ the projects */
  , aggregate = Seq(
      api
    , benchmark
    , cli
    , core
    , data
    , extract
    , generate
    , ingest
    , mr
    , scoobi
    , storage
    , validate
    , alien_hdfs
    )
  )
  /* this should only ever export _api_, DO NOT add things to this list */
  .dependsOn(api)

  lazy val standardSettings = Defaults.defaultSettings ++
                              projectSettings          ++
                              compilationSettings      ++
                              testingSettings          ++
                              Seq[Settings](
                                resolvers := depend.resolvers
                              )

  lazy val projectSettings: Seq[Settings] = Seq(
    name := "ivory"
  , version in ThisBuild := s"""1.0.0-${Option(System.getenv("HADOOP_VERSION")).getOrElse("cdh5")}"""
  , organization := "com.ambiata"
  , scalaVersion := "2.10.4"
  , fork in run  := true
  ) ++ Seq(prompt)

  def lib(name: String) =
    promulgate.library(s"com.ambiata.ivory.$name", "ambiata-oss")

  def app(name: String) =
    promulgate.all(s"com.ambiata.ivory.$name", "ambiata-oss", "ambiata-dist")

  lazy val api = Project(
    id = "api"
  , base = file("ivory-api")
  , settings = standardSettings ++ lib("api") ++ Seq[Settings](
      name := "ivory-api"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value))
  )
  .dependsOn(generate, ingest, validate, extract)

  lazy val benchmark = Project(
    id = "benchmark"
  , base = file("ivory-benchmark")
  , settings = standardSettings ++ app("benchmark") ++ Seq[Settings](
      name := "ivory-benchmark"
    , fork in run := true
    , javaOptions in run <++= (fullClasspath in Runtime).map(cp => Seq("-cp", sbt.Attributed.data(cp).mkString(":")))
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.caliper)
  )
  .dependsOn(api)

  lazy val cli = Project(
    id = "cli"
  , base = file("ivory-cli")
  , settings = standardSettings ++ app("cli") ++ Seq[Settings](
      name := "ivory-cli"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.scoobi(version.value))
  )
  .dependsOn(api)

  lazy val core = Project(
    id = "core"
  , base = file("ivory-core")
  , settings = standardSettings ++ lib("core") ++ Seq[Settings](
      name := "ivory-core"
    , addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0-M3" cross CrossVersion.full)
    ) ++ Seq[Settings](libraryDependencies ++= depend.trove ++ depend.scalaz ++ depend.mundane ++ depend.joda ++ depend.specs2 ++ depend.thrift ++ depend.hadoop(version.value) ++ Seq(
        "org.scala-lang" % "scala-compiler" % "2.10.4"
      , "org.scala-lang" % "scala-reflect" % "2.10.4"
      , "org.scalamacros" % "quasiquotes_2.10.3" % "2.0.0-M3"
    ))
  )

  lazy val data = Project(
    id = "data"
  , base = file("ivory-data")
  , settings = standardSettings ++ lib("data") ++ Seq[Settings](
      name := "ivory-data"
    , addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0-M3" cross CrossVersion.full)
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane ++ depend.specs2 ++ depend.hadoop(version.value) ++ Seq(
        "org.scala-lang" % "scala-compiler" % "2.10.4"
      , "org.scala-lang" % "scala-reflect" % "2.10.4"
      , "org.scalamacros" % "quasiquotes_2.10.3" % "2.0.0-M3"
    ))
  )

  lazy val extract = Project(
    id = "extract"
  , base = file("ivory-extract")
  , settings = standardSettings ++ lib("extract") ++ Seq[Settings](
      name := "ivory-extract"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value))
  )
  .dependsOn(core, scoobi, storage, validate)

  lazy val generate = Project(
    id = "generate"
  , base = file("ivory-generate")
  , settings = standardSettings ++ lib("generate") ++ Seq[Settings](
      name := "ivory-generate"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.joda ++ depend.scoobi(version.value))
  )
  .dependsOn(core, storage)

  lazy val ingest = Project(
    id = "ingest"
  , base = file("ivory-ingest")
  , settings = standardSettings ++ lib("ingest") ++ Seq[Settings](
      name := "ivory-ingest"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.joda ++ depend.specs2 ++ depend.scoobi(version.value) ++ depend.saws)
  )
  .dependsOn(core, storage, alien_hdfs, scoobi, mr)

  lazy val mr = Project(
    id = "mr"
  , base = file("ivory-mr")
  , settings = standardSettings ++ lib("mr") ++ Seq[Settings](
      name := "ivory-mr"
    ) ++ Seq[Settings](libraryDependencies ++= depend.thrift ++ depend.mundane ++ depend.scalaz ++ depend.specs2 ++ depend.hadoop(version.value))
  )
  .dependsOn(core, alien_hdfs)

  lazy val scoobi = Project(
    id = "scoobi"
  , base = file("ivory-scoobi")
  , settings = standardSettings ++ lib("scoobi") ++ Seq[Settings](
      name := "ivory-scoobi"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value) ++ depend.saws)
  )
  .dependsOn(core, alien_hdfs)

  lazy val storage = Project(
    id = "storage"
  , base = file("ivory-storage")
  , settings = standardSettings ++ lib("storage") ++ Seq[Settings](
      name := "ivory-storage"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value) ++ depend.saws)
  )
  .dependsOn(core, data, scoobi, alien_hdfs, core % "test->test")

  lazy val validate = Project(
    id = "validate"
  , base = file("ivory-validate")
  , settings = standardSettings ++ lib("validate") ++ Seq[Settings](
      name := "ivory-validate"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value) ++ depend.specs2)
  )
  .dependsOn(core, scoobi, storage)

  lazy val alien_hdfs = Project(
    id = "hdfs"
  , base = file("ivory-alien-hdfs")
  , settings = standardSettings ++ lib("alien.hdfs") ++ Seq[Settings](
      name := "ivory-alien-hdfs"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.hadoop(version.value) ++ depend.mundane ++ depend.saws ++ depend.scoobi(version.value) ++ depend.specs2)
  )

  lazy val compilationSettings: Seq[Settings] = Seq(
    javaOptions ++= Seq("-Xmx3G", "-Xms512m", "-Xss4m")
  , javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
  , maxErrors := 20
  , scalacOptions ++= Seq("-target:jvm-1.6", "-deprecation", "-unchecked", "-feature", "-language:_", "-Ywarn-all", "-Xlint", "-Xfatal-warnings", "-Yinline-warnings")
  )

  lazy val testingSettings: Seq[Settings] = Seq(
    initialCommands in console := "import org.specs2._"
  , logBuffered := false
  , cancelable := true
  , fork in test := true
  , javaOptions += "-Xmx3G"
  )

  lazy val prompt = shellPrompt in ThisBuild := { state =>
    val name = Project.extract(state).currentRef.project
    (if (name == "ivory") "" else name) + "> "
  }

  lazy val buildAssemblySettings: Seq[Settings] = Seq(
    artifact in (Compile, assembly) ~= { art =>
      art.copy(`classifier` = Some("assembly"))
    }
  ) ++ addArtifact(artifact in (Compile, assembly), assembly)

}
