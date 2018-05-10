package de.hpi.ads.database.types

import java.nio.ByteBuffer

import scala.reflect.runtime.universe._
import boopickle.Default._

class ListType(dataType: DataType) extends DataType {
    type InternalType = List[dataType.InternalType]
    val tag: TypeTag[InternalType] = typeTag[InternalType]

    def fromBytes(binaryData: ByteBuffer): Any = Unpickle[List[dataType.InternalType]].fromBytes(binaryData)
}

object ListType {
    def apply(dataType: DataType) = new ListType(dataType)
}
