organization := "com.github.worldline-messaging"

name := "akka-persistence-kafka"

version := "0.9.2"

scalaVersion := "2.12.3"

crossScalaVersions := Seq("2.12.3")

resolvers += "worldline-messaging at bintray" at "https://dl.bintray.com/worldline-messaging-org/maven"

parallelExecution in Test := false

publishArtifact in Test := true

libraryDependencies ++= Seq(
  "com.google.protobuf"  % "protobuf-java"                 % "2.6.1",
  "com.typesafe.akka"   %% "akka-persistence"              % "2.5.31",
  "com.typesafe.akka"   %% "akka-persistence-tck"          % "2.5.31" % Test,
  "com.typesafe.akka"   %% "akka-testkit"                  % "2.5.31" % Test,
  "org.apache.kafka"    %% "kafka"                         % "2.2.0",
  "org.apache.kafka"    %% "kafka"                         % "2.2.0"  % Test classifier "test",
  "org.apache.kafka"         % "kafka-clients"            % "2.2.0",
  "org.apache.kafka"         % "kafka-clients"            % "2.2.0" % Test classifier "test",
  "org.apache.curator"   % "curator-test"                  % "3.3.0"    % Test,
  "org.slf4j" 		 % "slf4j-log4j12" 		   % "1.7.25"	% Test,
  "com.typesafe.akka" 	%% "akka-slf4j" 		   % "2.5.31"	% Test
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
