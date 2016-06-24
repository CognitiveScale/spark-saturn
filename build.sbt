import com.c12e.sbt.Build._
import com.c12e.sbt.dependencies._
import com.c12e.sbt.plugins.NativePackagerPlugin
import com.c12e.sbt.plugins.ClasspathCheckPlugin
import com.c12e.sbt.plugins.StaticChecksPlugin.
  { JavacCheck, ScalacCheck, Wart, javacChecksIgnored, scalacChecksIgnored,
    wartsIgnored }

lazy val root =
  app("spark-saturn")
    .project
    .enablePlugins(NativePackagerPlugin)
    .disablePlugins(ClasspathCheckPlugin)
    .settings(
      javacChecksIgnored(JavacCheck.processing),
      scalacChecksIgnored(
        ScalacCheck.deadCode,
        ScalacCheck.unused,
        ScalacCheck.unusedImport,
        ScalacCheck.docDetached,
        ScalacCheck.numericWiden,
        ScalacCheck.valueDiscard),
      wartsIgnored(
        Wart.AsInstanceOf,
        Wart.DefaultArguments,
        Wart.FinalCaseClass,
        Wart.IsInstanceOf,
        Wart.ListOps,
        Wart.MutableDataStructures,
        Wart.NonUnitStatements,
        Wart.Null,
        Wart.OptionPartial,
        Wart.Option2Iterable,
        Wart.Product,
        Wart.Serializable,
        Wart.ToString,
        Wart.TryPartial,
        Wart.Var),
     libraryDependencies ++=
        List(
          "org.apache.spark" %% "spark-core" % "1.6.1",
          "org.apache.spark" %% "spark-sql" % "1.6.1",
          "org.apache.avro" % "avro" % "1.7.7",
          "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.2",
          "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0",
//          "com.databricks" %% "spark-csv" % "1.4.0",
//          "com.stratio.datasource" %% "spark-mongodb" % "0.11.2",
          scalaTest
          ))
