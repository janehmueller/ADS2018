package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object BinaryType extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Array[Byte]]
        stream.writeInt(internalData.length)
        stream.write(internalData)
    }

    def readBytes(stream: ObjectInputStream): Any = {
        val numBytes = stream.readInt()
        val binaryData = new Array[Byte](numBytes)
        stream.read(binaryData)
    }
}
