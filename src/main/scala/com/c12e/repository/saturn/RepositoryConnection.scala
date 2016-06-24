package com.c12e.repository.saturn

import com.datastax.driver.core.{Cluster, Session}

/**
  *
  * @author msanchez at cognitivescale.com
  * @since 6/20/16
  */
abstract class RepositoryConnection(conf: RepositoryConf) extends Serializable {

  protected val cluster: Cluster = if (conf.username.isDefined && conf.password.isDefined)
    Cluster.builder.addContactPoint(conf.host.get).withCredentials(conf.username.get, conf.password.get).build
  else Cluster.builder.addContactPoint(conf.host.get).build

  protected val session: Session = cluster.connect

  def close(): Unit = {
    session.close()
    cluster.close()
  }

  def keyspace(): String = RepositoryConnection.KEYSPACE

  def dataTableName(): String = RepositoryConnection.DATA_TABLE_NAME
}

object RepositoryConnection {
  val KEYSPACE: String = "saturn"
  val DATA_TABLE_NAME: String = "saturn_data"

  val COL_REPOSITORY_ID = "repositoryid"
  val COL_NAME = "name"
  val COL_KEY_ID = "keyid"
  val COL_TIMESTAMP = "timestamp"
  val COL_DATA = "data"
  val COL_SCHEMA_ID = "schemaid"

  def initTables(conf: RepositoryConf): Unit = {
    val cluster: Cluster = if (conf.username.isDefined && conf.password.isDefined)
      Cluster.builder.addContactPoint(conf.host.get).withCredentials(conf.username.get, conf.password.get).build
    else Cluster.builder.addContactPoint(conf.host.get).build

    val session: Session = cluster.connect

    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
    session.execute(s"USE $KEYSPACE")

    /// Create the saturn data table
    session.execute(s"CREATE TABLE IF NOT EXISTS $DATA_TABLE_NAME ("
      + s"$COL_REPOSITORY_ID text,"
      + s"$COL_NAME          text,"
      + s"$COL_TIMESTAMP     timeuuid,"
      + s"$COL_KEY_ID        text,"
      + s"$COL_SCHEMA_ID     text,"
      + s"$COL_DATA          blob,"
      + s"PRIMARY KEY ( ($COL_REPOSITORY_ID, $COL_NAME), $COL_TIMESTAMP, $COL_KEY_ID )" + ");")

    session.close()
    cluster.close()
  }

  def dropTables(conf: RepositoryConf): Unit = {
    val cluster: Cluster = if (conf.username.isDefined && conf.password.isDefined)
      Cluster.builder.addContactPoint(conf.host.get).withCredentials(conf.username.get, conf.password.get).build
    else Cluster.builder.addContactPoint(conf.host.get).build

    val session: Session = cluster.connect

    session.execute(s"USE $KEYSPACE")
    session.execute(s"DROP TABLE $DATA_TABLE_NAME")
    session.execute(s"DROP KEYSPACE $KEYSPACE")

    session.close()
    cluster.close()
  }
}
