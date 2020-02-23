package mlstream.testutils

object TestUtils {
  case class TimedResult[T](result: T, timeMillisecs: Long)
  def timed[R](msg: String, codeBlock: => R): TimedResult[R] = {
    val start = System.currentTimeMillis()
    val result = codeBlock // call-by-name
    val end = System.currentTimeMillis()
    val elapsed = end - start
    val timeAsStr =
      if (elapsed >= 1000) (elapsed / 1000.0 + " secs.") else (elapsed + " ms.")
    println(s"Time for '${msg}' ${timeAsStr}")
    TimedResult(result, elapsed)
  }
}
