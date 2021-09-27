organization := "com.github.krasserm"

name := "akka-persistence-kafka"

version := "0.6"

scalaVersion := "2.12.3"

crossScalaVersions := Seq("2.10.4", "2.11.6", "2.12.1")

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

parallelExecution in Test := false

publishArtifact in Test := true

libraryDependencies ++= Seq(
  "com.google.protobuf"  % "protobuf-java"                 % "2.6.1",
  "com.typesafe.akka"   %% "akka-persistence"              % "2.5.3",
  "com.typesafe.akka"   %% "akka-persistence-tck"          % "2.5.3" % Test,
  "com.typesafe.akka"   %% "akka-testkit"                  % "2.5.3" % Test,
  "commons-io"           % "commons-io"                    % "2.5"      % Test,
  "org.apache.kafka"    %% "kafka"                         % "0.11.0.0",
  "org.apache.curator"   % "curator-test"                  % "3.3.0"    % Test,
  "org.slf4j" 		 % "slf4j-log4j12" 		   % "1.7.25"	% Test,
  "com.typesafe.akka" 	%% "akka-slf4j" 		   % "2.5.3"	% Test
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

publishTo := Some("GitHub Package Registry" at "https://maven.pkg.github.com/worldline-messaging/akka-persistence-kafka")
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials_github")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
