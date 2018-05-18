package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object DoubleType extends DataType {
    override def byteSize = 8

    override def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Double]
        stream.writeDouble(internalData)
    }

    override def readBytes(stream: ObjectInputStream): Double = {
        stream.readDouble
    }

    override def toBytes(data: Any): Array[Byte] = {
        val longValue = java.lang.Double.doubleToRawLongBits(data.asInstanceOf[Double])
        LongType.toBytes(longValue)
    }

    override def fromBytes(data: Array[Byte]): Double = {
        java.lang.Double.longBitsToDouble(LongType.fromBytes(data))
    }
}
