package com.c12e.repository.saturn

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author msanchez at cognitivescale.com
  * @since 6/19/16
  */
class SaturnRelation(@transient val sqlContext: SQLContext, params: Map[String, String], schemaProvided: Option[StructType] = None)
  extends BaseRelation with TableScan with InsertableRelation with Serializable {

  val conf = RepositoryConf.fromParams(params)

  override def schema: StructType = schemaProvided.getOrElse(RelationUtils.generateDataSchema(params))

  override def buildScan(): RDD[Row] = {
    var rddResult: RDD[Row] = sqlContext.sparkContext.emptyRDD
    try {
      val sat = createDeserializer()
      val ri: RecordIterator = sat.iterator

      while (ri.hasNext) {
        val recCount: Int = ri.getFetchCount
        val grList: ArrayBuffer[Row] = new ArrayBuffer[Row]()

        for (i <- 0 until recCount) {
          val recData: RecordIterator.RecordData = ri.next
          val flds: java.util.ArrayList[AnyRef] = RelationUtils.buildRow(recData.data)

          /// Add the saturn metadata to the result
          /// NOTE: The order you add these is important, and must match the order defined in the schema
          flds.add(recData.repositoryId)
          flds.add(recData.name)
          flds.add(recData.timestamp)
          flds.add(recData.keyid)
          flds.add(recData.schemaId)

          // NOTE: Scala syntax :_* - expands an array into a vararg (works with Scala or Java varargs)
          // SEE http://stackoverflow.com/questions/6051302/what-does-colon-underscore-star-do-in-scala
          val r: Row = RowFactory.create(flds.toArray:_*)
          grList += r
        }
        if (rddResult == null) rddResult = sqlContext.sparkContext.parallelize(grList)
        else rddResult = rddResult.union(sqlContext.sparkContext.parallelize(grList))
      }
      sat.close()
    }
    catch {
      case e: UnsupportedTypeException =>
        e.printStackTrace()
    }
    rddResult
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    //    val sc = sqlContext.sparkContext
    //    val recCount: Accumulator[Integer] = sc.accumulator(0)

    val name: String = params("name")
    val rowSchema: StructType = data.schema

    data.foreachPartition((rows) => {
      val writeSchema: Schema = SchemaConverter.generateAvroSchema(name, rowSchema)
      val sat = createSerializer()

      try {
        var rowCount = 0

        while (rows.hasNext) {
          val r = rows.next()
          val bldr = RelationUtils.generateAvroRecord(r, writeSchema)
          val record = bldr.build()
          sat.serializeRecord(record)

          rowCount += 1
        }

        //        recCount.add(rowCount);
      }
      catch {
        case e: SchemaIncompatibleException => e.printStackTrace()
      }

      sat.close()
    })

    //    val log: SaturnUpdateLog = new SaturnUpdateLog(name, dtype, version, ctx.sparkContext.applicationId)
    //    log.signalUpdate(tstamp, recCount.value)
  }

  def isEmptyVersion: Boolean = {
    true
  }

  private def createDeserializer(): SaturnDeserializer = new SaturnDeserializer(conf)

  private def createSerializer(): SaturnSerializer = {
    val timeStamp: UUID = UUIDs.timeBased
    new SaturnSerializer(conf, timeStamp)
  }
}
