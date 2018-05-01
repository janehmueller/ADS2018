package de.hpi.ads
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Props
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.hpi.ads.actors.InterfaceActor.{CreateTableMessage, InsertRowMessage, SelectWhereMessage}
import de.hpi.ads.actors._
import de.hpi.ads.database.types.TableSchema

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object Main {
    def main(args: Array[String]): Unit = {
        tests()
    }

    def tests() = {
        val actorSystem = ActorSystem("Actor Database System")
        val interfaceActor = actorSystem.actorOf(Props[InterfaceActor], name = "interfaceActor")
        implicit val timeout = Timeout(5 seconds) // needed for `?` below

        createTable(interfaceActor, "Movies", new TableSchema("id;title"))
        insertValues(interfaceActor, "Movies", Array(Array(1, "movie1"), Array(2, "movie2")))
        if (compareResults(selectValues(interfaceActor, "Movies", Array("title"), Array(("id", 1))), Array(Array("movie1")))) {
            println("Test successful")
        } else {
            println("Test failed")
        }
    }

    //creates one table
    def createTable(actor: ActorRef, tableName: String, schema: TableSchema) = {
        val createTableF: Future[Boolean] = (actor ? CreateTableMessage(tableName, schema)).mapTo[Boolean]
        createTableF onComplete {
            case Success(b) => println("Table " + tableName + " created successfully!")
            case Failure(t) => println("An error has occurred during table creation: " + t.getMessage)
        }
    }

    //inserts many rows into table
    def insertValues(actor: ActorRef, tableName: String, data: Array[Array[Any]]) = {
        val futures = (1 to data.length).map { i =>
            actor ? InsertRowMessage(tableName, data(i))
        }
        Await.result(Future.sequence(futures), Duration.Inf)
    }

    //executes one select on table
    def selectValues(actor: ActorRef, tableName: String, selectedColumns: Array[String], conditions: Array[String Tuple2 Any]) = {
        val selectValF: Future[Array[Array[Any]]] = (actor ? SelectWhereMessage(tableName, selectedColumns, conditions)).mapTo
        var res = Array[Array[Any]]()
        selectValF onComplete {
            case Success(result) => res = result
            case Failure(t) => println("An error has occurred during value selection: " + t.getMessage)
        }
        res
    }

    def compareResults(result1: Array[Array[Any]] , result2: Array[Array[Any]]) = {
        result1.deep == result2.deep
    }
}
