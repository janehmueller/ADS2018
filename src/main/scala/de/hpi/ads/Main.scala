package de.hpi.ads

import akka.actor.ActorSystem

object Main {
    def main(args: Array[String]): Unit = {
        // create actor system
        val actorSystem = ActorSystem("Actor Database System")

        // create starting actors of the actor system
//        val actor = actorSystem.actorOf(Props[Actor], name = "actor")
//        val actorWithArgs = actorSystem.actorOf(Props(new Actor(arg1, arg2)), name = "actor")

        // send a start message to actor(s)
//        actor ! Message
    }
}
