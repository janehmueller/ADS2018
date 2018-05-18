package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

case class StringType(length: Int = 255) extends DataType {
    // TODO use length
    def byteSize: Int = length * 4 // TODO support UTF-8 or only ASCII?

    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[String]
        stream.writeObject(internalData)
    }

    def readBytes(stream: ObjectInputStream): String = {
        stream.readObject().asInstanceOf[String]
    }

    def toBytes(data: Any): Array[Byte] = {
        data.asInstanceOf[String].getBytes()
    }

    def fromBytes(data: Array[Byte]): String = {
        new String(data)
    }
}
