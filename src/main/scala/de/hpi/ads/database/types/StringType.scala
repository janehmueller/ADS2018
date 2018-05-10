package de.hpi.ads.database.types

import java.nio.ByteBuffer

import scala.reflect.runtime.universe._
import boopickle.Default._

object StringType extends DataType {
    type InternalType = String
    val tag: TypeTag[InternalType] = typeTag[InternalType]

    def fromBytes(binaryData: ByteBuffer): Any = Unpickle[String].fromBytes(binaryData)
}
