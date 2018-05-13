package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

trait DataType {
    // TODO handle null values with Options when writing and reading
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit

    def readBytes(stream: ObjectInputStream): Any
}
