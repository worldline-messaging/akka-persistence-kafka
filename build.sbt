organization := "com.github.worldline-messaging"

name := "akka-persistence-kafka"

version := "0.9.5"

scalaVersion := "2.13.2"

crossScalaVersions := Seq("2.13.2")

resolvers += "worldline-messaging at bintray" at "https://dl.bintray.com/worldline-messaging-org/maven"

parallelExecution in Test := false

publishArtifact in Test := true

val akkaVersion = "2.6.5"
val kafkaVersion = "2.4.1"

libraryDependencies ++= Seq(
  "com.google.protobuf"  % "protobuf-java"                 % "2.6.1",
  "com.typesafe.akka"   %% "akka-persistence"              % akkaVersion
    exclude ("org.scala-lang.modules", "scala-java8-compat_2.12"),
  "com.typesafe.akka"   %% "akka-persistence-tck"          % akkaVersion % Test,
  "com.typesafe.akka"   %% "akka-testkit"                  % akkaVersion % Test,
  "org.apache.kafka"    %% "kafka"                         % kafkaVersion,
  "org.apache.kafka"    %% "kafka"                         % kafkaVersion  % Test classifier "test",
  "org.apache.kafka"         % "kafka-clients"            % kafkaVersion,
  "org.apache.kafka"         % "kafka-clients"            % kafkaVersion % Test classifier "test",
  "org.apache.curator"   % "curator-test"                  % "3.3.0"    % Test,
  "org.slf4j" 		 % "slf4j-log4j12" 		   % "1.7.28"	% Test,
  "com.typesafe.akka" 	%% "akka-slf4j" 		   % akkaVersion	% Test
)

/*artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  val lastCommit = git.gitHeadCommit.value
  val suffix = {
    if (module.revision.trim.endsWith("-SNAPSHOT") && lastCommit.isDefined)
      s"${module.revision.split('-')(0)}-${lastCommit.get}"
    else module.revision
  }

  artifact.name + "_" + sv.binary + "-" + suffix + "." + artifact.extension
}*/

publishTo := Some("Bintray API Realm" at "https://api.bintray.com/maven/worldline-messaging-org/maven/akka-persistence-kafka;publish=1")
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
