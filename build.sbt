
val version : String = "2.12"
val zioVersion = "2.0.4"

scalaVersion := "2.12.15"


libraryDependencies += "dev.zio" % s"zio_2.12" % "2.0.4"
libraryDependencies += "dev.zio" % "zio-test_2.12" % "2.0.4" % "test"


lazy val root = (project in file("."))
  .enablePlugins(ScriptedPlugin)
  .settings(
    name := "Project",
    scriptedLaunchOpts ++= List("-Xms1024m", "-Xmx1024m", "-XX:ReservedCodeCacheSize=128m", "-Xss2m", "-Dfile.encoding=UTF-8"),
    resolvers += Resolver.url("typesafe", url("https://repo.typesafe.com/typesafe/ivy-releases/"))(Resolver.ivyStylePatterns)
  )