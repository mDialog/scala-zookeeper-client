name := "scala-zookeeper-client"

organization := "com.mdialog"

version := "3.2.5"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-unchecked", "-deprecation")

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "1.7.1" % "test",
  "org.slf4j" % "slf4j-api" % "1.6.4",
  "org.slf4j" % "slf4j-log4j12" % "1.6.4",
  "log4j" % "log4j" % "1.2.16",
  "org.apache.zookeeper" % "zookeeper" % "3.3.6" excludeAll(
    ExclusionRule(name = "jms"),
    ExclusionRule(name = "jmxtools"),
    ExclusionRule(name = "jmxri")
  )
)

credentials += Credentials(Path.userHome / ".mdialog.credentials")

resolvers ++= Seq(
    "mDialog snapshots" at "http://artifactory.mdialog.com/artifactory/snapshots",
    "mDialog releases" at "http://artifactory.mdialog.com/artifactory/releases",
    "repo1" at "http://repo1.maven.org/maven2/",
    "jboss-repo" at "http://repository.jboss.org/maven2/",
    "apache" at "http://people.apache.org/repo/m2-ibiblio-rsync-repository/",
    "twitter.com" at "http://maven.twttr.com/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases"
)

publishTo <<= version { (v: String) =>
  if (v.trim.endsWith("-SNAPSHOT"))
    Some("snapshots" at "http://artifactory.mdialog.com/artifactory/snapshots")
  else
    Some("releases" at "http://artifactory.mdialog.com/artifactory/releases")
}
