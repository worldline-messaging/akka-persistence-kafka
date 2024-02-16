package akka.persistence.kafka.stress.fixtures

import akka.persistence.PersistentActor
import RestartableActor._

//Just an integration of https://github.com/tudorzgureanu/akka-persistence-playground.git

trait RestartableActor extends PersistentActor {

  abstract override def receiveCommand: Receive = super.receiveCommand orElse {
    case RestartActor => throw RestartActorException
  }
}

object RestartableActor {
  case object RestartActor

  private object RestartActorException extends Exception
}