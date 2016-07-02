resolvers +=
  "c12e-read-maven" at
    "http://cognitivescale.artifactoryonline.com/cognitivescale/all-maven/"

addSbtPlugin("com.c12e" % "c12e-sbt-plugin" % "0.0.156-g65bae48")

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

//addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.3")