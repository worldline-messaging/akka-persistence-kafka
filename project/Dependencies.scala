import sbt._

object Version {
  val akka  = "2.5.22"
  val kafka = "2.2.0"
}

object Dependencies {
  lazy val scalaTest          = "org.scalatest"       %% "scalatest"            % "3.0.5"
  lazy val protobufJava       = "com.google.protobuf" % "protobuf-java"         % "2.6.1"
  lazy val akkaPersistence    = "com.typesafe.akka"   %% "akka-persistence"     % Version.akka
  lazy val akkaPersistenceTck = "com.typesafe.akka"   %% "akka-persistence-tck" % Version.akka % Test
  lazy val akkaTestkit        = "com.typesafe.akka"   %% "akka-testkit"         % Version.akka % Test
  lazy val kafka              = "org.apache.kafka"    %% "kafka"                % Version.kafka
  lazy val kafkaClients       = "org.apache.kafka"    % "kafka-clients"         % Version.kafka
  //https://stackoverflow.com/questions/25195583/kafka-jar-does-not-include-kafka-utils-testutils
  lazy val kafkaTest        = "org.apache.kafka"   %% "kafka"        % Version.kafka % Test classifier "test"
  lazy val kafkaClientsTest = "org.apache.kafka"   % "kafka-clients" % Version.kafka % Test classifier "test"
  lazy val curatorTest      = "org.apache.curator" % "curator-test"  % "4.2.0"       % Test
  lazy val slf4jLog4j       = "org.slf4j"          % "slf4j-log4j12" % "1.7.26"      % Test
  lazy val akkaSlf4j        = "com.typesafe.akka"  %% "akka-slf4j"   % Version.akka  % Test
}
