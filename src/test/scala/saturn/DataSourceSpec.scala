package saturn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author msanchez at cognitivescale.com
  * @since 6/22/16
  */
class DataSourceSpec extends FlatSpec with Matchers {

  val newsRepoOpts = Map(
    "host" -> "localhost",
//    "username" -> "csdev",
//    "password" -> "w3stBr4k3r",
    "repositoryId" -> "saturn_test",
    "name" -> "news_articles"
  )

  "DataFrame Save" should "persist to the Saturn repository" in {
    val conf = new SparkConf().setAppName("DataSourceSpec").setMaster("local[1]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    import sqlContext.implicits._

    val articles: DataFrame = sc.parallelize(List(
      new NewsArticle("1", "Article 1", "This is article 1"),
      new NewsArticle("2", "Article 2", "This is article 2"),
      new NewsArticle("3", "Article 3", "This is article 3")
    )).toDF()

    articles.write.format("com.c12e.repository.saturn")
      .options(newsRepoOpts)
      .option("version", "1")
      .save()
  }

  "DataFrame Load" should "load data from the Saturn repository" in {
    val conf = new SparkConf().setAppName("DataSourceSpec").setMaster("local[1]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val articles: DataFrame = sqlContext.read.format("com.c12e.repository.saturn")
      .options(newsRepoOpts)
      .load()

    articles.printSchema()

    articles.count() shouldBe 3
  }
}
