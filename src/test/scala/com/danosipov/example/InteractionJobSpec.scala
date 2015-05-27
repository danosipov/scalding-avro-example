package com.danosipov.example

import com.twitter.scalding._
import org.scalatest.{FunSuite, Matchers}

/**
 * Test suite for the InteractionJob
 */
class InteractionJobSpec extends FunSuite with Matchers with FieldConversions {
  test("Job should produce expected output for test input") {
    val input = io.Source.fromFile("src/test/resources/interactions").getLines()
      .flatMap((line: String) => List(line.hashCode -> line)).toList
    JobTest(manifest[InteractionJob])
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(TextLine("inputFile"), input)
      .sink[(String, Int)](TypedCsv[(String, Int)]("outputFile")) { outputBuffer =>
        outputBuffer.contains(("8c56f864-ca4a-449e-b467-6cea8d780d42", 3)) shouldBe true
        outputBuffer.contains(("9122accb-4f78-4762-8400-d395df99f277", 1)) shouldBe true
      }
      .run
      .finish
  }
}
