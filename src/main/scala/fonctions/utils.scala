package fonctions

import org.apache.spark.sql.{DataFrame}
import constants._
import org.apache.spark.sql.functions._

object utils {


  def portDump(anonym: String, port_dump: String, debut: String, fin: String): DataFrame = spark.sql(
    s"""
      |WITH
      |temp_port_dump AS (
      |SELECT
      |     num_ligne,
      |     pd_rangeholder,
      |     pd_recipientid,
      |     pd_route,
      |     filename,
      |     ingest_date,
      |     year,
      |     month,
      |     day
      |FROM $port_dump
      |WHERE day BETWEEN '$debut' AND '$fin'
      |),
      |
      |temp_anonym AS (
      |SELECT
      |     msisdn,
      |     msisdn_h
      |FROM $anonym
      |)
      |
      |SELECT
      |       b.msisdn_h AS msisdn,
      |       a.pd_rangeholder,
      |       a.pd_recipientid,
      |       a.pd_route,
      |       a.filename,
      |       a.ingest_date,
      |       a.year,
      |       a.month,
      |       a.day
      |FROM temp_port_dump a
      |INNER JOIN temp_anonym b
      |ON a.num_ligne = b.msisdn
    """.stripMargin)


  def pretupsC2s(pretups: String, date: String): DataFrame = {
    val df1 = spark.sql(
      s"""
        |SELECT * FROM $pretups
        |WHERE day = '$date'
      """.stripMargin)

    df1
      .withColumn("sender_msisdn", sha2(col("sender_msisdn"), 256))
      .withColumn("recevier_msisdn", sha2(col("recevier_msisdn"), 256))
      .drop("num_ligne", "num_corresp")

  }


}
