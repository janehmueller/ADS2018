package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.Date

object DateType extends DataType {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = {
        val internalData = data.asInstanceOf[Date]
        stream.writeLong(internalData.getTime)
    }

    def readBytes(stream: ObjectInputStream): Any = {
        new Date(stream.readLong)
    }
}
