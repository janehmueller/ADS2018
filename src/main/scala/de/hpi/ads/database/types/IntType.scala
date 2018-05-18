package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object IntType extends DataType {
    override def byteSize = 4

    override def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Int]
        stream.writeInt(internalData)
    }

    override def readBytes(stream: ObjectInputStream): Int = {
        stream.readInt
    }

    override def toBytes(data: Any): Array[Byte] = {
        val intValue = data.asInstanceOf[Int]
        Array(
            intValue >> 24,
            intValue >> 16,
            intValue >> 8,
            intValue >> 0
        ).map(_ & 0xff)
        .map(_.asInstanceOf[Byte])
    }

    override def fromBytes(data: Array[Byte]): Int = {
        val intData = data.map(0xff & _)
        intData(0) << 24  |
            intData(1) << 16  |
            intData(2) << 8   |
            intData(3) << 0
    }
}
