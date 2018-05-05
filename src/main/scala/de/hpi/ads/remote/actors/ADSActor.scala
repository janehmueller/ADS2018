package de.hpi.ads.remote.actors

import akka.actor.{Actor, ActorLogging}

trait ADSActor extends Actor with ActorLogging {
    override def preStart(): Unit = {
        super.preStart()
        this.log.info(s"Started actor: ${this.self}")
    }

    override def postStop(): Unit = {
        super.postStop()
        this.log.info(s"Stopped actor: ${this.self}")
    }
}
