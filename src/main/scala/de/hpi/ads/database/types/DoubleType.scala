package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object DoubleType extends DataType {
    def byteSize = 8

    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Double]
        stream.writeDouble(internalData)
    }

    def readBytes(stream: ObjectInputStream): Double = {
        stream.readDouble
    }

    def toBytes(data: Any): Array[Byte] = {
        val longValue = java.lang.Double.doubleToRawLongBits(data.asInstanceOf[Double])
        LongType.toBytes(longValue)
    }

    def fromBytes(data: Array[Byte]): Double = {
        java.lang.Double.longBitsToDouble(LongType.fromBytes(data))
    }
}
