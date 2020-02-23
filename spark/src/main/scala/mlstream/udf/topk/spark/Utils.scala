package mlstream.udf.topk.spark

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.{ SparkSession, UDFRegistration }

object Utils {
  def getRegistry(sparkSession: SparkSession): FunctionRegistry = {
    val field = classOf[UDFRegistration].getDeclaredField("functionRegistry")
    val isAccessible = field.isAccessible
    field.setAccessible(true)
    val funcReg = field.get(sparkSession.udf).asInstanceOf[FunctionRegistry]
    field.setAccessible(isAccessible)
    funcReg
  }

  def registerSparkUDF(
      sparkSession: SparkSession,
      name: String,
      builder: FunctionRegistry.FunctionBuilder): Unit = {
    val funcReg = getRegistry(sparkSession)
    funcReg.createOrReplaceTempFunction(name, builder)
  }

  def unregisterSparkUDF(sparkSession: SparkSession, name: String): Unit = {
    val funcReg = getRegistry(sparkSession)
    funcReg.dropFunction(FunctionIdentifier(name))
  }

}
