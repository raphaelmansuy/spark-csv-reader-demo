
val version : String = "2.12"

val zioVersion = "2.0.9"
val sparkVersion = "3.3.2"

scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "dev.zio" % s"zio_${scalaBinaryVersion.value}" % zioVersion,
  "dev.zio" % s"zio-test_${scalaBinaryVersion.value}" % zioVersion % "test"
)


lazy val root = (project in file("."))
  .enablePlugins(ScriptedPlugin)
  .settings(
    name := "Project",
    scriptedLaunchOpts ++= List("-Xms1024m", "-Xmx1024m", "-XX:ReservedCodeCacheSize=128m", "-Xss2m", "-Dfile.encoding=UTF-8"),
    resolvers += Resolver.url("typesafe", url("https://repo.typesafe.com/typesafe/ivy-releases/"))(Resolver.ivyStylePatterns)
  )