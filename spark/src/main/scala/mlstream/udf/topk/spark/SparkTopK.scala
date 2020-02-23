package mlstream.udf.topk.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  ImperativeAggregate,
  TypedImperativeAggregate
}
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  ExpressionDescription
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{
  TypeCheckFailure,
  TypeCheckSuccess
}
import org.apache.spark.sql.functions.lit

object SparkTopK {
  val topk = "approx_topk"
  val extract_topk = "extract_approx_topk"

  lazy val helpString = s"Function $topk accepts from 1 to 4 arguments"

  def approx_topk(
      key: Column,
      value: Column,
      count: Int,
      storage: String): Column = {
    val impl = new SparkTopK(
      List(key.expr, value.expr, lit(count).expr, lit(storage).expr),
      0,
      0)
    new Column(impl.toAggregateExpression())
  }

  def extract_approx_topk(topk: Column): Column = {
    val impl = ExtractTopKItems(topk.expr)
    new Column(impl)
  }

  def _topk(inputs: Seq[Expression]): Expression = {
    new SparkTopK(inputs.toList, 0, 0)
  }

  def _extractItems(inputs: Seq[Expression]): Expression = {
    // TODO: VERIFY THAT ARGS IS JUST 1
    val asLst = inputs.toList
    ExtractTopKItems(asLst(0))
  }

  def registerUDFs(sparkSession: SparkSession): Unit = {
    Utils.registerSparkUDF(sparkSession, topk, _topk)
    Utils.registerSparkUDF(sparkSession, extract_topk, _extractItems)
  }

  def unregisterUDFs(sparkSession: SparkSession): Unit = {
    Utils.unregisterSparkUDF(sparkSession, topk)
    Utils.unregisterSparkUDF(sparkSession, extract_topk)
  }
}

@ExpressionDescription(
  usage =
    """
    _FUNC_(key, value, top_k_items, max_memory) - Returns the most frequent top_k_items
    in a dataframe. Uses a maximum of max_memory in MBs or GBs.
  """)
case class SparkTopK(
    inputsExpr: List[Expression],
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[SparkAbstractTopK] {

  lazy val topKFactory =
    SparkTopKParamsValidator.getSparkTopKFactory(inputsExpr)

  override def children: Seq[Expression] = {
    inputsExpr
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (topKFactory.isFailure) {
      val msg = topKFactory.failed.get.getMessage
      TypeCheckFailure(msg)
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = topKFactory.get.returnType

  override def nullable: Boolean = false

  override def createAggregationBuffer(): SparkAbstractTopK = {
    topKFactory.get.createAggregationBuffer()
  }

  override def update(
      state: SparkAbstractTopK,
      input: InternalRow): SparkAbstractTopK = {
    val f = topKFactory.get
    val keyExpr = f.keyExpr
    val valueExpr = f.valueExpr
    state.update(keyExpr, valueExpr, input)
  }

  override def merge(
      buffer: SparkAbstractTopK,
      input: SparkAbstractTopK): SparkAbstractTopK = {
    buffer.mergeFrom(input)
  }

  override def eval(buffer: SparkAbstractTopK): Any = {
    topKFactory.get.eval(buffer)
  }

  override def serialize(buffer: SparkAbstractTopK): Array[Byte] = {
    buffer.serialize()
  }

  override def deserialize(storageFormat: Array[Byte]): SparkAbstractTopK = {
    topKFactory.get.deserialize(storageFormat)
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = SparkTopK.topk
}
