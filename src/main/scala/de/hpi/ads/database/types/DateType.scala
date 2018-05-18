package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.Date

object DateType extends DataType {
    def byteSize = 8

    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Date]
        stream.writeLong(internalData.getTime)
    }

    def readBytes(stream: ObjectInputStream): Date = {
        new Date(stream.readLong)
    }

    override def toBytes(data: Any): Array[Byte] = {
        val longValue = data.asInstanceOf[Date].getTime
        LongType.toBytes(longValue)
    }

    def fromBytes(data: Array[Byte]): Date = {
        new Date(LongType.fromBytes(data))
    }
}
