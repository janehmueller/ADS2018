package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

case class StringType(length: Int = 255) extends DataType {
    // TODO use length
    override def byteSize: Int = length * 4 // TODO support UTF-8 or only ASCII?

    override def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[String]
        stream.writeObject(internalData)
    }

    override def readBytes(stream: ObjectInputStream): String = {
        stream.readObject().asInstanceOf[String]
    }

    override def toBytes(data: Any): Array[Byte] = {
        data.asInstanceOf[String].getBytes()
    }

    override def fromBytes(data: Array[Byte]): String = {
        new String(data)
    }

    override def lessThan(a: Any, b: Any): Boolean = a.asInstanceOf[String] < b.asInstanceOf[String]
}
