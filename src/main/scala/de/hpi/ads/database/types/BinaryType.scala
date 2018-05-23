package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

case class BinaryType(length: Int = 255) extends DataType {
    // TODO use length
    override def byteSize: Int = length

    override def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Array[Byte]]
        stream.writeInt(internalData.length)
        stream.write(internalData)
    }

    override def readBytes(stream: ObjectInputStream): Array[Byte] = {
        val numBytes = stream.readInt()
        val binaryData = new Array[Byte](numBytes)
        stream.readFully(binaryData)
        binaryData
    }

    override def toBytes(data: Any): Array[Byte] = data.asInstanceOf[Array[Byte]]

    override def fromBytes(data: Array[Byte]): Array[Byte] = data

    //TODO override def lessThan(a: Any, b: Any): Boolean = a.asInstanceOf[Int] < b.asInstanceOf[Int]
}
