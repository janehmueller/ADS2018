package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object LongType extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Long]
        stream.writeLong(internalData)
    }

    def readBytes(stream: ObjectInputStream): Any = {
        stream.readLong
    }
}
