package com.puroguramingu.features

import org.apache.spark.rdd.RDD

case class RDDStats[T <: Iterable](original: RDD[T]) {
  lazy val empty = original.isEmpty()

  def means: Array[Double] = {
    if(empty) return Array[Double]()

    val n = original.first().size

    val means = new Array[Double](n)
    val counts = new Array[Int](means.length)

    original.aggregate(means.zip(counts))(
      // Compute the average within a RDD partition
      (agg, row) => {
        row.zipWithIndex.foreach{ case (e, i) => {
          if (isMissing(e)) {
            val delta = e - agg(i)._1
            val n = agg(i)._2 + 1
            agg(i) = (agg(i)._1 + delta / n, n)
          }
        }}
        agg
      },
      // Merge all RDD partition stats
      (agg1, agg2) => {
        merge(agg1, agg2)
      }
    ).map(_._1)
  }

  private def merge(agg1: Array[(Double, Int)], agg2: Array[(Double, Int)]): Array[(Double, Int)] = {
    agg1.indices.foreach { idx =>
      if (agg1(idx)._2 == 0) {
        agg1(idx) = agg2(idx)
      } else {
        val otherMu: Double = agg2(idx)._1
        val mu: Double = agg1(idx)._1
        val n = agg1(idx)._2
        val otherN = agg2(idx)._2
        val delta = otherMu - mu

        if (otherN * 10 < n) {
          agg1(idx) = (mu + (delta * otherN) / (n + otherN), n + otherN)
        } else if (n * 10 < otherN) {
          agg1(idx) = (otherMu - (delta * n) / (n + otherN), n + otherN)
        } else {
          agg1(idx) = ((mu * n + otherMu * otherN) / (n + otherN), n + otherN)
        }
      }
    }
    agg1
  }

}
