package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object DoubleType extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Double]
        stream.writeDouble(internalData)
    }

    def readBytes(stream: ObjectInputStream): Any = {
        stream.readDouble
    }
}
