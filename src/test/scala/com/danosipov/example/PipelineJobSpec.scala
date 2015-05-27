package com.danosipov.example

import java.nio.ByteBuffer

import com.twitter.algebird.{HyperLogLog, HyperLogLogMonoid}
import com.twitter.scalding._
import com.twitter.scalding.avro.PackedAvroSource
import org.scalatest._

/**
 * Test suite for the InteractionJob
 */
class PipelineJobSpec extends FunSuite with Matchers with FieldConversions {
  test("Job should produce expected output for test input") {
    val input = io.Source.fromFile("src/test/resources/interactions").getLines()
      .flatMap((line: String) => List(line.hashCode -> line)).toList
    val avroResources = List(
      Interaction.newBuilder()
        .setUserid("8c56f864-ca4a-449e-b467-6cea8d780d42")
        .setInteractions(
          ByteBuffer.wrap(HyperLogLog.toBytes(
            new HyperLogLogMonoid(16).apply("d96d8397-4231-42a3-b0c8-0acc0a385e6e".getBytes("UTF-8"))))) // User already interacted with this ID, count shouldn't change
        .build(),
      Interaction.newBuilder()
        .setUserid("9122accb-4f78-4762-8400-d395df99f277")
        .setInteractions(
          ByteBuffer.wrap(HyperLogLog.toBytes(
            new HyperLogLogMonoid(16).apply("newID".getBytes("UTF-8"))))) // User hasn't interacted with this ID in the new input
        .build()
    )

    JobTest(manifest[PipelineJob])
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .arg("avro_input", "avroFile")
      .arg("avro_output", "avroOutput")
      .source(TextLine("inputFile"), input)
      .source(PackedAvroSource[Interaction]("avroFile"), avroResources)
      .sink[(String, Long)](TypedCsv[(String, Long)]("outputFile")) { outputBuffer =>
        outputBuffer.contains(("8c56f864-ca4a-449e-b467-6cea8d780d42", 3)) shouldBe true
        outputBuffer.contains(("9122accb-4f78-4762-8400-d395df99f277", 2)) shouldBe true
      }
      .sink[Interaction](PackedAvroSource[Interaction]("avroOutput")) { out =>
        // Verify the checkpoint we're writing out
        out.exists { i =>
          i.getUserid == "8c56f864-ca4a-449e-b467-6cea8d780d42" &&
            HyperLogLog.fromBytes(i.getInteractions.array()).approximateSize.estimate == 3
        } shouldBe true
      }
      .run
      .finish
  }
}
