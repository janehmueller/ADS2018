package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object LongType extends DataType {
    def byteSize = 8

    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Long]
        stream.writeLong(internalData)
    }

    def readBytes(stream: ObjectInputStream): Long = {
        stream.readLong
    }

    def toBytes(data: Any): Array[Byte] = {
        val longValue = data.asInstanceOf[Long]
        Array(
            longValue >> 56,
            longValue >> 48,
            longValue >> 40,
            longValue >> 32,
            longValue >> 24,
            longValue >> 16,
            longValue >> 8,
            longValue >> 0
        ).map(_ & 0xff)
        .map(_.asInstanceOf[Byte])
    }

    def fromBytes(data: Array[Byte]): Long = {
        val longData = data
            .map(0xff & _)
            .map(_.asInstanceOf[Long])
        longData(0) << 56  |
            longData(1) << 48  |
            longData(2) << 40  |
            longData(3) << 32  |
            longData(4) << 24  |
            longData(5) << 16  |
            longData(6) << 8   |
            longData(7) << 0
    }
}
