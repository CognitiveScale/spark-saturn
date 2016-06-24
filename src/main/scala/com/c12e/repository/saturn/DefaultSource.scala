package com.c12e.repository.saturn

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  *
  * @author msanchez at cognitivescale.com
  * @since 6/19/16
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    RepositoryConnection.initTables(RepositoryConf.fromParams(parameters))
    new SaturnRelation(sqlContext, parameters)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    RepositoryConnection.initTables(RepositoryConf.fromParams(parameters))
    new SaturnRelation(sqlContext, parameters)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    RepositoryConnection.initTables(RepositoryConf.fromParams(parameters))
    val relation = new SaturnRelation(sqlContext, parameters)

    mode match{
      case Append         => relation.insert(data, overwrite = false)
      case Overwrite      => relation.insert(data, overwrite = true)
      case ErrorIfExists  => if(relation.isEmptyVersion) relation.insert(data, overwrite = false)
      else throw new UnsupportedOperationException("Writing in a non-empty version.")
      case Ignore         => if(relation.isEmptyVersion) relation.insert(data, overwrite = false)
    }

    relation
  }
}
