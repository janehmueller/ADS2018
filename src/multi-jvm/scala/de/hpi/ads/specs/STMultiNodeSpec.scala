package de.hpi.ads.specs

import scala.language.implicitConversions
import akka.remote.testkit.{MultiNodeSpec, MultiNodeSpecCallbacks}
import org.scalatest._

/**
  * Hooks up MultiNodeSpec with ScalaTest
  */
trait STMultiNodeSpec extends MultiNodeSpecCallbacks
    with FlatSpecLike with Matchers with BeforeAndAfterAll { self: MultiNodeSpec =>

    override def beforeAll(): Unit = multiNodeSpecBeforeAll()

    override def afterAll(): Unit = multiNodeSpecAfterAll()
}
