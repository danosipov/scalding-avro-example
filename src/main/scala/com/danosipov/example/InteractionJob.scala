package com.danosipov.example

import java.util.Date

import com.twitter.scalding._

/**
 * Simple job to calculate the total of one-way interactions from users.
 */
class InteractionJob(args: Args) extends Job(args) {
  type UserID = String
  val inputPath = args("input")
  val outputPath = args("output")

  val interactions: TypedPipe[(Date, UserID, UserID)] = TypedPipe.from(TextLine(inputPath))
    .map { line =>
      val splitLine = line.split("\t")
      (new Date(splitLine(0).toLong), splitLine(1), splitLine(2))
    }

  val interactionsWithCounts = interactions
    .groupBy(_._2) // Group by primary User ID
    .mapValueStream { iterator =>
      // Get the set of users interacted with, make a set out of it (to eliminate duplicates)
      Iterator(iterator.map(_._3).toSet.size)
    }
    .toTypedPipe

  interactionsWithCounts
    .write(TypedCsv(outputPath))
}
