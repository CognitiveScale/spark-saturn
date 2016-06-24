# Saturn Data Repository for Apache Spark

A secure and distributed storage service for DataFrames.  Currently built on top of Apache Cassandra.  Integrates with 
Apache Spark as a Spark Data Source and works across all support Spark programming languages (Java/Scala, Python, R).

## Benefits
Like a Git repository for your DataFrames.  Supports version history and managed/partitioned storage of DataFrames by storage container (called repositories) and name.

## Example Use
In Scala:

    val sc = SparkContext.getOrCreate()
    val sqlContext = SqlContext.getOrCreate(sc)
    
    // Load the products DataFrame from the repository - this will load the 'tip' revision
    val products = sqlContext.read.
        .format("com.c12e.repository.saturn")
        .option("host", "localhost")
        .option("repository", "products")
        .option("name", "acme_products")
        .load()
        
    // Create a new DataFrame - just containing Men's shirts
    val shirts = products.filter("categoryNamePath LIKE 'Clothing & Accessories > Men > Shirts%'")
    
    // Store the new DataFrame
    shirts.write.format("com.c12e.repository.saturn")
        .option("host", "localhost")
        .option("repository", "products")
        .option("name", "acme_mens_shirts")
        .save()
        
## Getting Started
TBD