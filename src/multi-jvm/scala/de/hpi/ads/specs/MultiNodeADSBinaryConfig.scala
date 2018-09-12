package de.hpi.ads.specs

import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

object MultiNodeADSBinaryConfig extends MultiNodeConfig {
    val node1: RoleName = role("thor01")
    val node2: RoleName = role("thor02")
    val node3: RoleName = role("thor03")
    val node4: RoleName = role("thor04")

    private val adsConfigString = """
                                    |akka.cluster.auto-join = off
                                    |akka.actor.provider = cluster
                                    |ads.hierarchyMode = binary
                                    |ads.maxChildren = 5
                                    |akka.actor.warn-about-java-serializer-usage = false
                                  """.stripMargin.trim()
    commonConfig(ConfigFactory.parseString(adsConfigString))
}
