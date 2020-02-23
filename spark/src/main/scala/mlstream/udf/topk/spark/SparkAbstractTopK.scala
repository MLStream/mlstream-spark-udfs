package mlstream.udf.topk.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

abstract class SparkAbstractTopK {
  def update(
      keyExpr: Expression,
      valueExpr: Expression,
      input: InternalRow): SparkAbstractTopK
  def mergeFrom(input: SparkAbstractTopK): SparkAbstractTopK
  def eval(numItems: Int): Any
  def serialize(): Array[Byte]
}

abstract class SparkAbstractTopKFactory extends Serializable {
  def eval(topK: SparkAbstractTopK): Any
  def deserialize(storageFormat: Array[Byte]): SparkAbstractTopK
  def createAggregationBuffer(): SparkAbstractTopK

  def returnType: DataType

  def keyExpr: Expression
  def valueExpr: Expression
}
