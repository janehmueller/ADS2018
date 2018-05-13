package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.ListBuffer

case class ListType(dataType: DataType) extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[List[Any]]
        val numEntries = internalData.length
        stream.writeInt(numEntries)
        internalData.foreach(dataType.writeBytes(_, stream))
    }

    def readBytes(stream: ObjectInputStream): Any = {
        val numEntries = stream.readInt
        var readData = ListBuffer[Any]()
        var index = 0
        while(index < numEntries) {
            readData += dataType.readBytes(stream)
            index += 1
        }
        readData.toList
    }
}
