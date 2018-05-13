package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object StringType extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[String]
        stream.writeObject(internalData)
    }

    def readBytes(stream: ObjectInputStream): Any = {
        stream.readObject().asInstanceOf[String]
    }
}
