package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object BooleanType extends DataType {
    def byteSize = 1 // TODO is this true?

    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Boolean]
        stream.writeBoolean(internalData)
    }

    def readBytes(stream: ObjectInputStream): Boolean = {
        stream.readBoolean
    }

    def toBytes(data: Any): Array[Byte] = {
        if (data.asInstanceOf[Boolean]) Array(1) else Array(0)
    }

    def fromBytes(data: Array[Byte]): Boolean = {
        val Array(value) = data
        value == 1
    }
}
