package com.puroguramingu.features

import com.puroguramingu.features.MissingValHandling.MissingValHandling
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * A wrapper around RDDs which exposes several methods imputing missing values in rows.
  * @param original The original RDD with missing values
  * @tparam T
  * @tparam E
  */
class MissingValRDD[T <: Iterable[E] : ClassTag, E](val original: RDD[T]) {

  lazy val stats: RDDStats[T] = RDDStats(original)

  /**
    * Replaces all null and [[scala.None]] values in each row with the mean value for the column in which they reside.
    *
    * This method will *only* work with numeric values.
    *
    * @return RDD with imputed values
    */
  def imputMean: RDD[T] = {
    original.map{ seq =>
      seq.zipWithIndex.map{ case(v,i) => {
        if(isMissing(v)) stats.means(i) else v
      } }.asInstanceOf[T]
    }
  }

  /**
    * Replaces all null and [[scala.None]] values in each row with the most common value in that column.
    * @return RDD with imputed values
    */
  def imputMostCommon: RDD[T] = ???

  /**
    * Replaces all null and [[scala.None]] values in each row using the strategy passed as the argument.
    * If no strategy was passed for the given column the default is used (MostCommon for non numeric values,
    * mean for numeric values).
    *
    * @param strategy Missing value handling strategy for each column.
    * @return RDD with imputed values
    */
  def imput(strategy: Map[Int, MissingValHandling]) = ???
}

object MissingValHandling extends Enumeration {
  type MissingValHandling = Value
  val Skip, Mean, Mode, MostCommon = Value
}