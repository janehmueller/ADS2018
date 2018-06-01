package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

object BooleanType extends DataType {
    override def byteSize = 1 // TODO is this true?

    override def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Boolean]
        stream.writeBoolean(internalData)
    }

    override def readBytes(stream: ObjectInputStream): Boolean = {
        stream.readBoolean
    }

    override def toBytes(data: Any): Array[Byte] = {
        if (data.asInstanceOf[Boolean]) Array(1) else Array(0)
    }

    override def fromBytes(data: Array[Byte]): Boolean = {
        val Array(value) = data
        value == 1
    }

    override def lessThan(a: Any, b: Any): Boolean = a.asInstanceOf[Boolean] < b.asInstanceOf[Boolean]

    override def max(values: Any*): Boolean = {
        values
            .map(_.asInstanceOf[Boolean])
            .exists(identity)
    }

    override def min(values: Any*): Boolean = {
        values
            .map(_.asInstanceOf[Boolean])
            .forall(identity)
    }
}
