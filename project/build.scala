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
  , aggregate = Seq(api, cli, core, example, generate, ingest, metadata, repository, scoobi, snapshot, storage, thrift, validate, alien_hdfs)
  )
  .dependsOn(api, cli, core, example, generate, ingest, metadata, repository, scoobi, snapshot, storage, thrift, validate, alien_hdfs)

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
  ) ++ Seq(prompt)

  def lib(name: String) =
    promulgate.library(s"com.ambiata.ivory.$name", "ambiata-oss")

  def app(name: String) =
    promulgate.all(s"com.ambiata.ivory.$name", "ambiata-oss", "ambiata-dist")

  lazy val cli = Project(
    id = "cli"
  , base = file("ivory-cli")
  , settings = standardSettings ++ app("cli") ++ Seq[Settings](
      name := "ivory-cli"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz)
  )
  .dependsOn(generate, ingest, repository, snapshot, validate, scoobi, core, storage)

  lazy val api = Project(
    id = "api"
  , base = file("ivory-api")
  , settings = standardSettings ++ lib("api") ++ Seq[Settings](
      name := "ivory-api"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz)
  )
  .dependsOn(generate, ingest, repository, snapshot, validate)

  lazy val core = Project(
    id = "core"
  , base = file("ivory-core")
  , settings = standardSettings ++ lib("core") ++ Seq[Settings](
      name := "ivory-core"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane ++ depend.joda ++ depend.specs2 ++ depend.hadoop(version.value)) // TODO: remove hadoop dep when a place to put repository Hdfs path handling is found
  )

  lazy val generate = Project(
    id = "generate"
  , base = file("ivory-generate")
  , settings = standardSettings ++ lib("generate") ++ Seq[Settings](
      name := "ivory-generate"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.joda)
  )
  .dependsOn(core, storage)

  lazy val example = Project(
    id = "example"
  , base = file("ivory-example")
  , settings = standardSettings ++ lib("example") ++ Seq[Settings](
      name := "ivory-example"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.joda ++ depend.saws ++ depend.scoobi(version.value))
  )
  .dependsOn(generate, repository, storage, core, ingest, scoobi, snapshot)

  lazy val ingest = Project(
    id = "ingest"
  , base = file("ivory-ingest")
  , settings = standardSettings ++ lib("ingest") ++ Seq[Settings](
      name := "ivory-ingest"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.joda ++ depend.specs2 ++ depend.saws)
  )
  .dependsOn(core, storage, metadata, alien_hdfs, scoobi)

  lazy val metadata = Project(
    id = "metadata"
  , base = file("ivory-metadata")
  , settings = standardSettings ++ lib("metadata") ++ Seq[Settings](
      name := "ivory-metadata"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.hadoop(version.value))
  )
  .dependsOn(core, alien_hdfs)

  lazy val repository = Project(
    id = "repository"
  , base = file("ivory-repository")
  , settings = standardSettings ++ lib("repository") ++ Seq[Settings](
      name := "ivory-repository"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.saws)
  )
  .dependsOn(core, ingest, storage, snapshot, alien_hdfs)

  lazy val scoobi = Project(
    id = "scoobi"
  , base = file("ivory-scoobi")
  , settings = standardSettings ++ lib("scoobi") ++ Seq[Settings](
      name := "ivory-scoobi"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.scoobi(version.value) ++ depend.saws)
  )
  .dependsOn(core, thrift, alien_hdfs)

  lazy val snapshot = Project(
    id = "snapshot"
  , base = file("ivory-snapshot")
  , settings = standardSettings ++ lib("snapshot") ++ Seq[Settings](
      name := "ivory-snapshot"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz)
  )
  .dependsOn(core, scoobi, storage, validate)

  lazy val storage = Project(
    id = "storage"
  , base = file("ivory-storage")
  , settings = standardSettings ++ lib("storage") ++ Seq[Settings](
      name := "ivory-storage"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.saws)
  )
  .dependsOn(core, metadata, thrift, scoobi, alien_hdfs)

  lazy val thrift = Project(
    id = "thrift"
  , base = file("ivory-thrift")
  , settings = standardSettings ++ lib("thrift") ++ Seq[Settings](
      name := "ivory-thrift"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.thrift)
  )

  lazy val validate = Project(
    id = "validate"
  , base = file("ivory-validate")
  , settings = standardSettings ++ lib("validate") ++ Seq[Settings](
      name := "ivory-validate"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.specs2)
  )
  .dependsOn(core, scoobi, storage)

  lazy val alien_hdfs = Project(
    id = "hdfs"
  , base = file("ivory-alien-hdfs")
  , settings = standardSettings ++ lib("alien.hdfs") ++ Seq[Settings](
      name := "ivory-alien-hdfs"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.hadoop(version.value) ++ depend.mundane ++ depend.saws ++ depend.specs2)
  )

  lazy val compilationSettings: Seq[Settings] = Seq(
    javaOptions ++= Seq("-Xmx3G", "-Xms512m", "-Xss4m")
  , maxErrors := 20
  , scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:_", "-Ywarn-all", "-Xlint", "-optimise", "-Xfatal-warnings", "-Yinline-warnings")
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
