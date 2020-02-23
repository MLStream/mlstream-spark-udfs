package mlstream.udf.topk.spark

import mlstream.udf.topk.impl.{ AbstractTopKImpl, AbstractTopKImplFactory }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  FloatType,
  StructField,
  StructType
}

final case class SparkGenericTopKFactory[
    Clazz <: AbstractTopKImpl[Clazz, I, O],
    I,
    O](
    _keyExpr: Expression,
    _valueExpr: Expression,
    numItemsExpr: Expression,
    maxMemoryExpr: Expression,
    numItems: Int,
    maxNumItems: Int,
    factory: AbstractTopKImplFactory[Clazz],
    converter: SparkToBytesConverter[I, O])
    extends SparkAbstractTopKFactory {

  def keyExpr: Expression = _keyExpr
  def valueExpr: Expression = _valueExpr

  def returnType: DataType = {
    val keyType = keyExpr.dataType
    StructType(
      Seq(
        StructField(
          "keys",
          ArrayType(keyType, containsNull = false),
          nullable = false),
        StructField(
          "values",
          ArrayType(FloatType, containsNull = false),
          nullable = false),
        StructField("totalCount", FloatType, nullable = false),
        StructField("errorBound", FloatType, nullable = false)))
  }

  override def eval(topK: SparkAbstractTopK): Any = {
    topK.eval(numItems)
  }

  def deserialize(storageFormat: Array[Byte]): SparkAbstractTopK = {
    val impl = factory.deserialize(storageFormat)
    new SparkGenericTopKImpl[Clazz, I, O](impl, converter)
  }

  def createAggregationBuffer(): SparkAbstractTopK = {
    val impl = factory.newInstance(maxNumItems)
    new SparkGenericTopKImpl[Clazz, I, O](impl, converter)
  }

  def childExpressions: Seq[Expression] = {
    val exprs =
      List(keyExpr, valueExpr, numItemsExpr, maxMemoryExpr).filter(e =>
        e != null)
    exprs
  }

}

final class SparkGenericTopKImpl[Clazz <: AbstractTopKImpl[Clazz, I, O], I, O](
    val impl: Clazz,
    val converter: SparkToBytesConverter[I, O])
    extends SparkAbstractTopK {
  override def mergeFrom(input: SparkAbstractTopK): SparkAbstractTopK = {
    val otherImpl = input.asInstanceOf[SparkGenericTopKImpl[Clazz, I, O]].impl
    impl.merge(otherImpl)
    this
  }

  def eval(numItems: Int): Any = {
    val (
      keys: Array[O],
      values: Array[Float],
      totalCount: Float,
      errorBound: Float) =
      impl.getSummary(numItems, input => converter.convertBackToSpark(input))
    InternalRow(
      new GenericArrayData(keys),
      new GenericArrayData(values),
      totalCount,
      errorBound)
  }

  override def update(
      keyExpr: Expression,
      valueExpr: Expression,
      input: InternalRow): SparkAbstractTopK = {

    val key = converter.eval(keyExpr, input)
    if (key.isEmpty) {
      return this
    }

    var value: Float = 1.0f
    if (valueExpr != null) {
      val valueAny = valueExpr.eval(input)
      if (valueAny == null) {
        return this
      }
      value = valueAny.asInstanceOf[Float]
      if (value.isNaN) {
        throw new IllegalStateException("Dataset contains NaN values")
      }
      if (value.isInfinity) {
        throw new IllegalStateException("Dataset contains infinity values")
      }
    }

    impl.insertKeyValue(key.get, value)
    this
  }

  def serialize(): Array[Byte] = {
    impl.serialize()
  }

}
