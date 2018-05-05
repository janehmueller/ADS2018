package de.hpi.ads

import akka.actor.ActorSystem
import de.hpi.ads.remote.actors._
import de.hpi.ads.remote.actors.UserActor.{UserCreateTableMessage, UserInsertValuesMessage, UserSelectValuesMessage}

object Main {
    def main(args: Array[String]): Unit = {
        val actorSystem = ActorSystem("ActorDatabaseSystem")
        val interfaceActor = actorSystem.actorOf(InterfaceActor.props(), InterfaceActor.defaultName)
        val userActor = actorSystem.actorOf(UserActor.props(interfaceActor), UserActor.defaultName)
        userActor ! UserCreateTableMessage("movies", "id;title")
        userActor ! UserInsertValuesMessage("movies", List("1", "movie1"))
        userActor ! UserInsertValuesMessage("movies", List("2", "movie2"))
        userActor ! UserSelectValuesMessage("movies", List("title"), _.key == "1")
        while (true) {}
        actorSystem.terminate()
    }
}
