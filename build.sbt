organization := "com.github.krasserm"

name := "akka-persistence-kafka"

version := "0.6.2"

scalaVersion := "2.12.6"

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

parallelExecution in Test := false

publishArtifact in Test := false

libraryDependencies ++= Seq(
  "com.google.protobuf"  % "protobuf-java"                 % "2.6.1",
  "com.typesafe.akka"   %% "akka-persistence"              % "2.5.13",
  "com.typesafe.akka"   %% "akka-persistence-tck"          % "2.5.13" % Test,
  "com.typesafe.akka"   %% "akka-testkit"                  % "2.5.13" % Test,
  "commons-io"           % "commons-io"                    % "2.5"      % Test,
  "org.apache.kafka"    %% "kafka"                         % "0.11.0.0",
  "org.apache.curator"   % "curator-test"                  % "4.0.1"    % Test,
  "org.slf4j" 		 % "slf4j-log4j12" 		   % "1.7.25"	% Test,
  "com.typesafe.akka" 	%% "akka-slf4j" 		   % "2.5.13"	% Test
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

bintrayOrganization := Some("worldline-messaging-org")
publishTo := Some("Bintray API Realm" at "https://api.bintray.com/maven/worldline-messaging-org/maven/akka-persistence-kafka")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
