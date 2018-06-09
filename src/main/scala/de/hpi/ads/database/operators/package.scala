package de.hpi.ads.database

import de.hpi.ads.database.types.{ColumnType, TableSchema}
import scala.collection.mutable.{Map => MMap}

package object operators {
    trait Operator {
        def column: String

        def value: Any

        def columnType(schema: TableSchema): ColumnType = schema.columnByName(column)

        var compareFunction: (Any, Any) => Boolean = _

        def setCompareFunction(schema: TableSchema): Unit

        def compare(other: Any): Boolean = {
            this.compareFunction(other, value)
        }

        def compareAny(other: Any): Boolean = {
            other.getClass == value.getClass && this.compare(other)
        }

        def apply(row: Array[Byte], schema: TableSchema): Boolean = {
            this.setCompareFunction(schema)
            this.compareAny(Row.read(row, this.column, schema))
        }

        def apply(other: Any, schema: TableSchema): Boolean = {
            this.setCompareFunction(schema)
            this.compareAny(other)
        }

        def apply(index: MMap[Any, List[Long]], schema: TableSchema): List[Long] = {
            this.setCompareFunction(schema)
            index
                .filterKeys(key => this.compareFunction(key, value))
                .values
                .flatten
                .toList
        }

        def useKeyIndex(index: MMap[Any, Long], schema: TableSchema): List[Long] = {
            this.setCompareFunction(schema)
            index
                .filterKeys(key => this.compareFunction(key, value))
                .values
                .toList
        }
    }

    case class EqOperator(column: String, value: Any) extends Operator {
        override def setCompareFunction(schema: TableSchema): Unit = {
            this.compareFunction = columnType(schema).eq
        }

        override def useKeyIndex(index: MMap[Any, Long], schema: TableSchema): List[Long] = {
            index.get(value).toList
        }

        override def apply(index: MMap[Any, List[Long]], schema: TableSchema): List[Long] = {
            index.getOrElse(value, Nil)
        }
    }

    case class NeqOperator(column: String, value: Any) extends Operator {
        override def setCompareFunction(schema: TableSchema): Unit = {
            this.compareFunction = columnType(schema).neq
        }
    }

    case class LessThanOperator(column: String, value: Any) extends Operator {
        override def setCompareFunction(schema: TableSchema): Unit = {
            this.compareFunction = columnType(schema).lessThan
        }
    }

    case class LessThanEqOperator(column: String, value: Any) extends Operator {
        override def setCompareFunction(schema: TableSchema): Unit = {
            this.compareFunction = columnType(schema).lessThanEq
        }
    }

    case class GreaterThanOperator(column: String, value: Any) extends Operator {
        override def setCompareFunction(schema: TableSchema): Unit = {
            this.compareFunction = columnType(schema).greaterThan
        }
    }

    case class GreaterThanEqOperator(column: String, value: Any) extends Operator {
        override def setCompareFunction(schema: TableSchema): Unit = {
            this.compareFunction = columnType(schema).greaterThanEq
        }
    }

    case class InRangeOperator(column: String, lowerBound: Any, upperBound: Any) extends Operator {
        override def value: Any = None

        override def setCompareFunction(schema: TableSchema): Unit = {
            this.compareFunction = (other: Any, value: Any) => {
                columnType(schema).greaterThanEq(other, lowerBound) && columnType(schema).lessThanEq(other, upperBound)
            }
        }
    }
}
