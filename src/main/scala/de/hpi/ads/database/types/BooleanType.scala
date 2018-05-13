package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object BooleanType extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Boolean]
        stream.writeBoolean(internalData)
    }

    def readBytes(stream: ObjectInputStream): Any = {
        stream.readBoolean
    }
}
