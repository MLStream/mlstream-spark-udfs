package mlstream.udf.topk.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{
  CollectionGenerator,
  Expression,
  UnaryExpression
}
import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  ExprCode
}
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Extracts topk items from their data structure into a list similar to Spark's explode function
 * @param child
 */
case class ExtractTopKItems(child: Expression)
    extends UnaryExpression
    with CollectionGenerator
    with Serializable {
  override val inline: Boolean = false
  override val position: Boolean = true

  def matchesType: (Boolean, Option[DataType]) = {
    child.dataType match {
      case StructType(
          Array(
            StructField("keys", ArrayType(keysType, false), false, _),
            StructField("values", ArrayType(FloatType, false), false, _),
            StructField("totalCount", FloatType, false, _),
            StructField("errorBound", FloatType, false, _))) => {
        keysType match {
          case IntegerType | LongType | StringType | StructType(_) =>
            (true, Some(keysType))
          case _ => (false, None)
        }
      }
      case _ => (false, None)
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    matchesType match {
      case (true, Some(_)) => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"We can't support type ${child.dataType.catalogString}")
    }
  }

  // hive-compatible default alias
  override def elementSchema: StructType = {
    matchesType match {
      case (true, Some(keyType)) => {
        new StructType()
          .add("key", keyType, nullable = false)
          .add("value", FloatType, nullable = false)
          .add("error", FloatType, nullable = false)
      }
      case _ => throw new RuntimeException()
    }
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    matchesType match {
      case (true, Some(keyType)) => {
        val row = child.eval(input).asInstanceOf[InternalRow]
        val keys = row.getArray(0)
        val values = row.getArray(1).toFloatArray()
        val error = row.getFloat(3)
        keyType match {
          case IntegerType => {
            val intKeys = keys.toIntArray()
            intKeys.zip(values).map {
              case (k, v) => {
                InternalRow(k, v, error)
              }
            }
          }
          case LongType => {
            val longKeys = keys.toLongArray()
            longKeys.zip(values).map {
              case (k, v) => {
                InternalRow(k, v, error)
              }
            }
          }
          case StringType => {
            val stringKeys = keys.toArray[UTF8String](StringType)
            stringKeys.zip(values).map {
              case (k, v) => {
                InternalRow(k, v, error)
              }
            }
          }
          case StructType(_) => {
            val objectKeys = keys.toObjectArray(keyType)
            objectKeys.zip(values).map {
              case (k, v) => {
                InternalRow(k, v, error)
              }
            }
          }
          case _ => throw new RuntimeException()
        }
      }
      case _ => throw new RuntimeException()
    }
  }

  override def collectionType: DataType = {
    matchesType match {
      case (true, Some(keyType)) => {
        new StructType()
          .add("key", keyType, nullable = false)
          .add("value", FloatType, nullable = false)
          .add("error", FloatType, nullable = false)
      }
      case _ => throw new RuntimeException()
    }
  }

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }
}
