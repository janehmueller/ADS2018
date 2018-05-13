package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object IntType extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Int]
        stream.writeInt(internalData)
    }

    def readBytes(stream: ObjectInputStream): Any = {
        stream.readInt
    }
}
