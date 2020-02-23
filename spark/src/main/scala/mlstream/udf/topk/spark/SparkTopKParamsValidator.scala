package mlstream.udf.topk.spark

import mlstream.udf.topk.impl.{
  ByteArrayTopKImpl,
  IntKeyOps,
  LongKeyOps,
  NumericTopKImpl,
  NumericTopKImplFactory
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  UnsafeProjection,
  UnsafeRow
}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types.{
  DataType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  StringType,
  StructType
}

import scala.util.{ Failure, Success, Try }

trait SparkToBytesConverter[I, O] {
  def eval(expr: Expression, row: InternalRow): Option[I]
  def convertBackToSpark(u: I): O
}

trait AllowedKeyType {
  def getSparkTopKFactory(
      keyExpr: Expression,
      valueExpr: Expression,
      numItemsExpr: Expression,
      maxMemoryExpr: Expression,
      numItems: Int,
      maxItems: Int): SparkAbstractTopKFactory

}

case class IntKeyType() extends AllowedKeyType {
  def getSparkTopKFactory(
      keyExpr: Expression,
      valueExpr: Expression,
      numItemsExpr: Expression,
      maxMemoryExpr: Expression,
      numItems: Int,
      maxItems: Int): SparkAbstractTopKFactory = {

    SparkGenericTopKFactory[NumericTopKImpl[Int], Int, Int](
      keyExpr,
      valueExpr,
      numItemsExpr,
      maxMemoryExpr,
      numItems,
      maxItems,
      NumericTopKImplFactory[Int](IntKeyOps),
      IntSparkToBytesConverter())
  }

}

case class LongKeyType() extends AllowedKeyType {
  def getSparkTopKFactory(
      keyExpr: Expression,
      valueExpr: Expression,
      numItemsExpr: Expression,
      maxMemoryExpr: Expression,
      numItems: Int,
      maxItems: Int): SparkAbstractTopKFactory = {

    SparkGenericTopKFactory[NumericTopKImpl[Long], Long, Long](
      keyExpr,
      valueExpr,
      numItemsExpr,
      maxMemoryExpr,
      numItems,
      maxItems,
      NumericTopKImplFactory[Long](LongKeyOps),
      LongSparkToBytesConverter())
  }
}

case class StringKeyType() extends AllowedKeyType {
  def getSparkTopKFactory(
      keyExpr: Expression,
      valueExpr: Expression,
      numItemsExpr: Expression,
      maxMemoryExpr: Expression,
      numItems: Int,
      maxItems: Int): SparkAbstractTopKFactory = {

    SparkGenericTopKFactory[ByteArrayTopKImpl, Array[Byte], Any](
      keyExpr,
      valueExpr,
      numItemsExpr,
      maxMemoryExpr,
      numItems,
      maxItems,
      ByteArrayTopKImpl,
      StringSparkToBytesConverter())
  }
}
case class StructKeyType() extends AllowedKeyType {
  def getSparkTopKFactory(
      keyExpr: Expression,
      valueExpr: Expression,
      numItemsExpr: Expression,
      maxMemoryExpr: Expression,
      numItems: Int,
      maxItems: Int): SparkAbstractTopKFactory = {

    SparkGenericTopKFactory[ByteArrayTopKImpl, Array[Byte], Any](
      keyExpr,
      valueExpr,
      numItemsExpr,
      maxMemoryExpr,
      numItems,
      maxItems,
      ByteArrayTopKImpl,
      StructSparkToBytesConverter(keyExpr))
  }
}

final case class IntSparkToBytesConverter()
    extends SparkToBytesConverter[Int, Int] {
  def eval(expr: Expression, row: InternalRow): Option[Int] = {
    val r = expr.eval(row)
    if (r == null) {
      None
    } else {
      Some(r.asInstanceOf[Int])
    }
  }

  def convertBackToSpark(u: Int): Int = {
    u
  }
}

final case class LongSparkToBytesConverter()
    extends SparkToBytesConverter[Long, Long] {
  def eval(expr: Expression, row: InternalRow): Option[Long] = {
    val r = expr.eval(row)
    if (r == null) {
      None
    } else {
      Some(r.asInstanceOf[Long])
    }
  }

  def convertBackToSpark(u: Long): Long = {
    u
  }
}

final case class StringSparkToBytesConverter()
    extends SparkToBytesConverter[Array[Byte], Any] {
  def eval(expr: Expression, row: InternalRow): Option[Array[Byte]] = {
    val r = expr.eval(row)
    if (r == null) {
      None
    } else {
      Some(r.asInstanceOf[UTF8String].getBytes)
    }
  }

  def convertBackToSpark(u: Array[Byte]): Any = {
    UTF8String.fromBytes(u.asInstanceOf[Array[Byte]])
  }
}

final case class StructSparkToBytesConverter(keyExpr: Expression)
    extends SparkToBytesConverter[Array[Byte], Any] {
  val numFields = keyExpr.dataType.asInstanceOf[StructType].fields.length
  lazy val converter = UnsafeProjection.create(
    keyExpr.dataType.asInstanceOf[StructType].fields.map(_.dataType))

  def eval(expr: Expression, row: InternalRow): Option[Array[Byte]] = {
    val r = expr.eval(row)
    if (r == null) {
      None
    } else {
      val unsafeRow = converter.apply(r.asInstanceOf[InternalRow])
      Some(unsafeRow.getBytes)
    }
  }

  def convertBackToSpark(u: Array[Byte]): Any = {
    val bs: Array[Byte] = u.asInstanceOf[Array[Byte]]
    val unsafeRow = new UnsafeRow(numFields)
    unsafeRow.pointTo(bs, bs.length)
    unsafeRow
  }
}

object SparkTopKParamsValidator {
  val maxMemoryRegex = """(\d+)\s*(MB|GB)""".r

  def failed[T](msg: String): Failure[T] = {
    Failure(new Exception(msg))
  }

  def mustBe(e: => Boolean, msg: => String): Try[Boolean] = {
    if (e) {
      Success(true)
    } else {
      failed(msg)
    }
  }

  def mustNotBe(e: => Boolean, msg: => String): Try[Boolean] = {
    mustBe({
      !e
    }, msg)
  }

  def sparkExprToInt(expr: Expression, argName: String): Try[Int] = {
    try {
      Success(expr.eval().asInstanceOf[Int])
    } catch {
      case _: ClassCastException =>
        failed(s"Parameter ${argName} is not convertible to integer")
      case e: Throwable => Failure(e)
    }
  }

  def validateInt(
      value: Int,
      argName: String,
      minValue: Int,
      maxValue: Int): Try[Int] = {
    for {
      _ <- mustNotBe({
        value.isNaN
      }, s"Parameter ${argName} cannot be NaN")
      _ <- mustBe(
        value < minValue || value > maxValue,
        s"Parameter ${argName} must be between ${minValue} and ${maxValue}")
    } yield value
  }

  def sparkExprToString(expr: Expression, argName: String): Try[String] = {
    try {
      Success(expr.eval().asInstanceOf[UTF8String].toString)
    } catch {
      case _: ClassCastException =>
        failed(s"Parameter {argName} is not convertible to string")
      case e: Throwable => Failure(e)
    }
  }

  def tryParseNumItems(numItems: Expression): Try[Int] = {
    val argName = "numItems"
    val minValue = 1
    val maxValue = 1000000
    val defaultValue = 10
    if (numItems == null) {
      Success(defaultValue)
    } else {
      for {
        _ <- mustBe(
          numItems.foldable,
          s"Parameter ${argName} must evaluate to a constant before execution")
        value <- sparkExprToInt(numItems, argName)
        _ <- mustNotBe({
          value.isNaN
        }, s"Parameter ${argName} cannot be NaN")
        _ <- mustBe(
          value >= minValue && value <= maxValue,
          s"Parameter ${argName} must be between ${minValue} and ${maxValue}")
      } yield value
    }
  }

  def extractMaxMemoryComponents(
      value: String,
      argName: String): Try[(String, String)] = {
    value match {
      case maxMemoryRegex(memory, unit) => Success((memory, unit))
      case _                            => failed(s"Unable to understand the value of field ${argName}")
    }
  }

  def convertToInt(value: String, arg: String): Try[Int] = {
    try {
      Success(value.toInt)
    } catch {
      case _: NumberFormatException =>
        failed(s"Cannot convert value to integer in argument ${arg}")
      case e: Throwable => Failure(e)
    }
  }

  def convertToUnit(value: String, arg: String): Try[Int] = {
    value match {
      case "MB" => Success(1024 * 1024)
      case "GB" => Success(1024 * 1024 * 1024)
      case _    => failed(s"Argument ${arg} must end with MB or GB")
    }
  }

  def deriveMaxItems(
      value: Int,
      unit: Int,
      fieldName: String,
      perItemMemory: Int,
      overhead: Int): Try[Int] = {
    val maxInBytes: Long = 2L * 1024L * 1024L * 1024L
    val valueInBytes: Long = value.toLong * unit.toLong
    if (valueInBytes > maxInBytes) {
      failed(
        s"The specified value in field ${fieldName} exceeds the allowed limit of 2GB")
    } else {
      Success(Math.max(1, ((valueInBytes - overhead) / perItemMemory)).toInt)
    }
  }

  def tryConvertMaxMemoryMaxItems(maxMemory: Expression): Try[Int] = {
    val argName = "maxMemory"
    val perItemMemory = 8
    val overhead = 0
    val defaultMemoryMB = 50
    if (maxMemory == null) {
      deriveMaxItems(
        defaultMemoryMB,
        1024 * 1024,
        argName,
        perItemMemory,
        overhead)
    } else {
      for {
        _ <- mustBe(
          maxMemory.foldable,
          s"Parameter ${argName} must evaluate to a constant before execution")
        valuePairsStr <- sparkExprToString(maxMemory, argName)
        (valueStr, unitStr) <- extractMaxMemoryComponents(
          valuePairsStr,
          argName)
        value <- convertToInt(valueStr, argName)
        _ <- mustNotBe(value.isNaN, argName)
        _ <- mustBe(value >= 1 && value < 10240, argName)
        unit <- convertToUnit(unitStr, argName)
        maxItems <- deriveMaxItems(
          value,
          unit,
          argName,
          perItemMemory,
          overhead)
      } yield maxItems
    }
  }

  def tryGetSupportedKeyType(t: DataType): Try[AllowedKeyType] = {
    t match {
      case IntegerType => Success(IntKeyType())
      case LongType    => Success(LongKeyType())

      case StringType => Success(StringKeyType())
      case StructType(_) =>
        if (typeContainsMap(t)) {
          failed("When key is of type struct it cannot contain a map")
        } else {
          Success(StructKeyType())
        }
      case _ => failed("Key type can only be int, long, string or struct")
    }
  }

  def tryValidateKey(keyExpr: Expression): Try[AllowedKeyType] = {
    if (keyExpr == null) {
      failed("key expression must be specified")
    } else {
      tryGetSupportedKeyType(keyExpr.dataType)
    }
  }

  def typeContainsMap(t: DataType): Boolean = {
    // we want to do simply t.existsRecursively(_.isInstanceOf[MapType]))
    // but existsRecusively is private

    val method = classOf[DataType]
      .getDeclaredMethod("existsRecursively", classOf[(DataType) => Boolean])
    val isAccessible = method.isAccessible
    method.setAccessible(true)
    val pred = (dt: DataType) => dt.isInstanceOf[MapType]
    val result = method.invoke(t, pred)
    method.setAccessible(isAccessible)
    result.asInstanceOf[Boolean]
  }

  def tryValidateValue(valueExpr: Expression): Try[FloatType] = {
    if (valueExpr != null && valueExpr.dataType != FloatType) {
      failed(
        "The value expression must be float. Try casting the value as float: CAST(value AS FLOAT)")
    }
    Success(FloatType)
  }

  def tryGetExpr(
      exprList: List[Expression],
      pos: Int,
      nullable: Boolean): Try[Expression] = {
    val nullableExpr = if (pos >= exprList.length) {
      null
    } else {
      exprList(pos)
    }
    if (nullableExpr == null && !nullable) {
      failed(s"Expression at position ${pos + 1} is must be provided")
    } else {
      Success(nullableExpr)
    }
  }

  def getSparkTopKFactory(
      inputsExpr: List[Expression]): Try[SparkAbstractTopKFactory] = {
    for {
      keyExpr <- tryGetExpr(inputsExpr, 0, false)
      valueExpr <- tryGetExpr(inputsExpr, 1, true)
      numItemsExpr <- tryGetExpr(inputsExpr, 2, true)
      maxMemoryExpr <- tryGetExpr(inputsExpr, 3, true)
      allowedKeyType <- tryValidateKey(keyExpr)
      _ <- tryValidateValue(valueExpr)
      numItems <- tryParseNumItems(numItemsExpr)
      maxItems <- tryConvertMaxMemoryMaxItems(maxMemoryExpr)
    } yield allowedKeyType.getSparkTopKFactory(
      keyExpr,
      valueExpr,
      numItemsExpr,
      maxMemoryExpr,
      numItems,
      maxItems)
  }
}
