package de.hpi.ads.database.types

import java.nio.ByteBuffer
import java.util.Date

import scala.reflect.runtime.universe._
import boopickle.Default._

object DateType extends DataType {
    type InternalType = Date
    val tag: TypeTag[InternalType] = typeTag[InternalType]

    def fromBytes(binaryData: ByteBuffer): Any = Unpickle[Date].fromBytes(binaryData)
}
