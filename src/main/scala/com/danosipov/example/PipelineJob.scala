package com.danosipov.example

import java.nio.ByteBuffer
import java.util.Date

import cascading.tuple.Fields
import com.twitter.algebird.Aggregator.{prepareMonoid => sumAfter}
import com.twitter.algebird.{HLL, HyperLogLog, HyperLogLogMonoid}
import com.twitter.bijection.{AbstractBijection, Bijection, Injection}
import com.twitter.scalding._
import com.twitter.scalding.avro.PackedAvroSource

/**
 * Pipeline job that uses Avro checkpoints to keep aggregating counts across runs.
 * Approximate set called HyperLogLog is used as an approximation for the aggregations.
 * Furthermore, the sample shows how it can be serialized to Avro.
 */
class PipelineJob(args: Args) extends Job(args) {
  import TDsl._ // Needed to support conversion to TypedPipe

  type UserID = String
  val inputPath = args("input")
  val outputPath = args("output")
  val avroSource = args("avro_input")
  val avroSink = args("avro_output")

  // A "Bijection" from HyperLogLog to bytes.
  // This implicit variable tells Scalding how to translate from a HyperLogLog
  // object and array of bytes, and back.
  implicit val hll2Bytes: Bijection[HLL, Array[Byte]] =
    new AbstractBijection[HLL, Array[Byte]] {
      def apply(h: HLL) = HyperLogLog.toBytes(h)
      override def invert(in: Array[Byte]) = HyperLogLog.fromBytes(in)
    }
  implicit val hll = new HyperLogLogMonoid(16) // 16 byte HyperLogLog monoid instance

  val newInteractions = TypedPipe.from(TextLine(inputPath))
    .map { line =>
      val splitLine = line.split("\t")
      (new Date(splitLine(0).toLong), splitLine(1), splitLine(2))
    }
    .groupBy(_._2) // Group by primary User ID
    .mapValues(i => hll(i._3.getBytes("UTF-8"))) // Make the values of the group the ID interacted with, inserted into a HyperLogLog
    .sum

  val avroInteractions = PackedAvroSource[Interaction](avroSource)
    .read
    .mapTo(Fields.FIRST -> ('user, 'interactions)) { i: Interaction =>
      // Field.FIRST is a workaround for an issue, where the field is named 'Interaction in actual run,
      // but is indexed in test (at least the way the test is written). Since we
      // expect only one object in the source anyway, Fields.FIRST is a nice compromise.
      val hll = Injection.invert[HLL, Array[Byte]](i.getInteractions.array())
      (i.getUserid, hll.get)
    }
    .toTypedPipe[(String, HLL)]('user, 'interactions)
    .groupBy(_._1) // Group by User
    .aggregate(sumAfter(_._2)) // Aggregate the HyperLogLogs into one

  val interactions = avroInteractions.outerJoin(newInteractions).mapValues {
    // Outer Join produces a tuple of two options in the values
    // This partial function produces a single value by merging them
    case (None, None) => hll.zero
    case (Some(h1), None) => h1
    case (None, Some(h2)) => h2
    case (Some(h1), Some(h2)) => h1 + h2
  }

  // Write the calculated results
  interactions
    .mapValues(_.approximateSize.estimate) // Get the estimated size
    .toTypedPipe
    .write(TypedCsv(outputPath))

  // Write the checkpointed interactions for future pipeline run
  interactions
    .toTypedPipe
    .map { i =>
      Interaction.newBuilder()
        .setUserid(i._1)
        .setInteractions(ByteBuffer.wrap(Injection[HLL, Array[Byte]](i._2)))
        .build()
    }
    .write(PackedAvroSource[Interaction](avroSink))
}
