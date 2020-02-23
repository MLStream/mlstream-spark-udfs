package mlstream.udf.topk.impl

trait AbstractTopKImpl[SuperClass, InputKey, OutputKey] {
  def insertKeyValue(key: InputKey, value: Float): Unit
  def merge(other: SuperClass): SuperClass
  def getSummary(k: Int, converter: InputKey => OutputKey)
      : (Array[OutputKey], Array[Float], Float, Float)
  def serialize(): Array[Byte]
}

trait AbstractTopKImplFactory[
    InstanceClass <: AbstractTopKImpl[InstanceClass, _, _]] {
  def deserialize(storageFormat: Array[Byte]): InstanceClass
  def newInstance(numItems: Int): InstanceClass
}
