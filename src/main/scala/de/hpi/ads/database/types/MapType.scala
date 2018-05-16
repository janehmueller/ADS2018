package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.{Map => MMap}

case class MapType(keyType: DataType, valueType: DataType) extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Map[Any, Any]]
        val numEntries = internalData.size
        stream.writeInt(numEntries)
        internalData.foreach { case (key, value) =>
            keyType.writeBytes(key, stream)
            valueType.writeBytes(value, stream)
        }
    }

    def readBytes(stream: ObjectInputStream): Any = {
        val numEntries = stream.readInt
        val readData = MMap.empty[Any, Any]
        var index = 0
        while(index < numEntries) {
            val key = keyType.readBytes(stream)
            readData(key) = valueType.readBytes(stream)
            index += 1
        }
        readData.toMap
    }
}
