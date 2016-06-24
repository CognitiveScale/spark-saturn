package com.c12e.repository.saturn

/**
  *
  * @author msanchez at cognitivescale.com
  * @since 6/22/16
  */
case class RepositoryConf
(
  host: Option[String] = None,
  username: Option[String] = None,
  password: Option[String] = None,
  repositoryId: Option[String]= None,
  name: Option[String] = None,
  version: Option[String] = None,
  schemaId: Option[String] = None,
  schema: Option[String] = None,
  fetchSize: Option[Int] = Some(0)
) {

  def getHost: String = host.orNull
  def getUsername: String = username.orNull
  def getPassword: String = password.orNull
  def getRepositoryId: String = repositoryId.orNull
  def getName: String = name.orNull
  def getVersion: String = version.orNull
  def getSchemaId: String = schemaId.orNull
  def getSchema: String = schema.orNull
  def getFetchSize: Int = fetchSize.get
}

object RepositoryConf {

  // Configuration property names
  val HOST = "host"
  val USERNAME = "username"
  val PASSWORD = "password"
  val NAME = "name"
  val VERSION = "version"
  val SCHEMA_ID = "schemaId"
  val FETCH_COUNT = "fetchCount"
  val REPOSITORY_ID = "repositoryId"
  val SCHEMA = "schema"


  def fromParams(params: Map[String, String]): RepositoryConf = {
    RepositoryConf(
      Some(params.getOrElse(HOST, "localhost")),
      if (params.isDefinedAt(USERNAME)) Some(params(USERNAME)) else None,
      if (params.isDefinedAt(PASSWORD)) Some(params(PASSWORD)) else None,
      Some(params(REPOSITORY_ID)),
      Some(params(NAME)),
      if (params.isDefinedAt(VERSION)) Some(params(VERSION)) else None,
      if (params.isDefinedAt(SCHEMA_ID)) Some(params(SCHEMA_ID)) else None,
      if (params.isDefinedAt(SCHEMA)) Some(params(SCHEMA)) else None,
      Some(params.getOrElse(FETCH_COUNT, "0").toInt))
  }
}
