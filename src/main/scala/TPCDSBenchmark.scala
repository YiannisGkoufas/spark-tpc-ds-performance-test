import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class TPCDSBenchmark (val tpcdsRootDir: String, val tpcdsDatabaseName: String = "TPCDS1G") {

  // val tpcdsRootDir = "/opt/spark-tpc-ds-performance-test"
  val tpcdsWorkDir = s"/opt/spark/work"
  val tpcdsDdlDir = s"${tpcdsRootDir}/ddl/individual"
  val tpcdsGenDataDir = s"${tpcdsRootDir}/data"
  val tpcdsQueriesDir = s"${tpcdsRootDir}/queries"
  var totalTime: Long = 0
  println("TPCDS root directory is at : "+ tpcdsRootDir)
  println("TPCDS ddl scripts directory is at: " + tpcdsDdlDir)
  println("TPCDS data directory is at: "+ tpcdsGenDataDir)
  println("TPCDS queries directory is at: "+ tpcdsQueriesDir)
  
  def clearTableDirectory(tableName: String): Unit = {
      import sys.process._
      val commandStr1 = s"rm -rf spark-warehouse/${tpcdsDatabaseName.toLowerCase()}.db/${tableName}/*"
      val commandStr2 = s"rm -rf spark-warehouse/${tpcdsDatabaseName.toLowerCase()}.db/${tableName}"
      var exitCode = Process(commandStr1).!
      exitCode = Process(commandStr2).!
  }

  def createDatabase(spark: SparkSession): Unit = {
      spark.sql(s"DROP DATABASE IF EXISTS ${tpcdsDatabaseName} CASCADE")
      spark.sql(s"CREATE DATABASE ${tpcdsDatabaseName}")
      spark.sql(s"USE ${tpcdsDatabaseName}")
  }

  /**
  * Function to create a table in spark. It reads the DDL script for each of the
  * tpc-ds table and executes it on Spark.
  */
  def createTable(spark: SparkSession, tableName: String): Unit = {
    println(s"Creating table $tableName ..")
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    clearTableDirectory(tableName)  
    val (fileName, content) = 
      spark.sparkContext.wholeTextFiles(s"${tpcdsDdlDir}/$tableName.sql").collect()(0) 
      
    // Remove the replace for the .dat once it is fixed in the github repo
    val sqlStmts = content.stripLineEnd
      .replace('\n', ' ')
      .replace("${TPCDS_GENDATA_DIR}", tpcdsGenDataDir)
      .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").split(";")
    sqlStmts.map(stmt => spark.sql(stmt))    
  }  

  def runQuery(spark: SparkSession,
              queryStr: String,
              individual: Boolean = true,
              resultDir: String): Seq[(String, Double, Int, String)] = {
    val querySummary = ArrayBuffer.empty[(String, Double, Int, String)]  
    val queryName = s"${tpcdsQueriesDir}/query${queryStr}.sql"   
    val (_, content) = spark.sparkContext.wholeTextFiles(queryName).collect()(0)  
    val queries = content.split("\n")
      .filterNot (_.startsWith("--"))
      .mkString(" ").split(";")
    
    var cnt = 1  
    for (query <- queries)  {
    val start = System.nanoTime()
    val df = spark.sql(query)   
    val result = spark.sql(query).collect  
    val timeElapsed = (System.nanoTime() - start) / 1000000000
    val name = if (queries.length > 1) {
        s"query${queryStr}-${cnt}"
    } else {
        s"query${queryStr}"
    }  
    val resultFile = s"${resultDir}/${name}-notebook.res"  
    df.coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .mode("overwrite")
        .save(resultFile)
    totalTime = totalTime + timeElapsed
    
    querySummary += Tuple4.apply(name, timeElapsed, result.length, resultFile)
    cnt += 1                
    }
    querySummary 
  }

  // run function for each table in tables array
  def forEachTable(tables: Array[String], f: (String) => Unit): Unit = {
    for ( table <- tables) {
      try {
        f(table)
      } catch {
        case e: Throwable => {
          println("EXCEPTION!! " + e.getMessage())
          throw e
        }
      }
    }
  }

  def runIndividualQuery(spark: SparkSession, queryNum: Int, resultDir: String = tpcdsWorkDir ): DataFrame = {
    import spark.implicits._
    
    spark.sql(s"USE ${tpcdsDatabaseName}")
    val queryStr = "%02d".format(queryNum) 
    val testSummary = ArrayBuffer.empty[(String, Double, Int, String)] 
    try {      
      println(s"Running TPC-DS Query : $queryStr")  
      testSummary ++= runQuery(spark, queryStr, true, resultDir)
    } catch {
        case e: Throwable => {
            println("Error in query "+ queryNum + " msg = " + e.getMessage)
        }
    }      
    testSummary.toDF("QueryName","ElapsedTime","RowsReturned", "ResultLocation")
  }

  def runAllQueries(spark: SparkSession, resultDir: String = tpcdsWorkDir): DataFrame = {
    import spark.implicits._

    spark.sql(s"USE ${tpcdsDatabaseName}")
    val testSummary = ArrayBuffer.empty[(String, Double, Int, String)]    
    var queryErrors = 0
    for (i <- 1 to 99) {
      try{
        val queryStr = "%02d".format(i)
        println(s"Running TPC-DS Query : $queryStr")   
        testSummary ++= runQuery(spark, queryStr, false, resultDir)
      } catch {
        case e: Throwable => {
              println("Error in query "+ i + " msg = " + e.getMessage)
              queryErrors += 1
        }
      }
    }

    println("=====================================================")
    if ( queryErrors > 0) {
      println(s"Query execution failed with $queryErrors errors")
    } else {
      println("All TPC-DS queries ran successfully")
    }
    println (s"Total Elapsed Time so far: ${totalTime} seconds.")
    println("=====================================================")
    testSummary.toDF("QueryName","ElapsedTime","RowsReturned", "ResultLocation")
  }

  def displaySummary(summaryDF: DataFrame): Unit = {
      summaryDF.select("QueryName", "ElapsedTime", "RowsReturned").show(10000)
  }

  def displayResult(spark: SparkSession, queryNum: Int, summaryDF: DataFrame) = {
    val queryStr = "%02d".format(queryNum)
    // Find result files for this query number. For some queries there are
    // multiple result files. 
    val  files = summaryDF.where(s"queryName like 'query${queryStr}%'").select("ResultLocation").collect()
    for (file <- files) {
        val fileName = file.getString(0)
        val df = spark.read
          .format("csv")
          .option("header", "true") //reading the headers
          .option("mode", "DROPMALFORMED")
          .load(fileName)
        val numRows:Int = df.count().toInt
        df.show(numRows, truncate=false)
    }
  }

  def explainQuery(spark: SparkSession, queryNum: Int) = {
    val queryStr = "%02d".format(queryNum)  
    val queryName = s"${tpcdsQueriesDir}/query${queryStr}.sql"   
    val (_, content) = spark.sparkContext.wholeTextFiles(queryName).collect()(0)  
    val queries = content.split("\n")
      .filterNot (_.startsWith("--"))
      .mkString(" ").split(";")
      
    for (query <- queries)  {    
      spark.sql(query).explain(true) 
    }
  }

  def createTables(spark: SparkSession, tables: Array[String]): Unit = {

    // Create database
    createDatabase(spark)

    // Create table
    forEachTable(tables, table => createTable(spark, table))

    //
    validateTables(spark, tables)
  }

  def validateTables(spark: SparkSession, tables: Array[String]): Unit = {
    // Run a count query and get the counts
    val rowCounts = tables.map { table =>
      spark.table(table).count()
    }

    val expectedCounts = Array (
        6, 1441548, 1920800, 20, 300, 12, 86400,
        71763,  11718, 100000, 73049, 11745000, 
        35, 287514, 5, 719384, 144067, 50000, 7200,
        18000, 20, 2880404, 60, 30
    )

    var errorCount = 0;
    val zippedCountsWithIndex = rowCounts.zip(expectedCounts).zipWithIndex
    for ((pair, index) <- zippedCountsWithIndex) {
      if (pair._1 != pair._2) {
          println(s"""ERROR!! Row counts for ${tables(index)} does not match.
          Expected=${expectedCounts(index)} but found ${rowCounts(index)}""")
          errorCount += 1
      }
    }

    println("=====================================================")
    if ( errorCount > 0) {
      println(s"Load verification failed with $errorCount errors")
    } else {
      println("Loaded and verified the table counts successfully")
    }
    println("=====================================================")

  }

  def createTableAndRunIndividualQuery(spark: SparkSession, tables: Array[String], queryNum: Int, resultDir: String = tpcdsWorkDir): Unit = {
      import spark.implicits._

      createTables(spark, tables)

      spark.sql(s"USE ${tpcdsDatabaseName}")
       
      val querySummary = runIndividualQuery(spark, queryNum, resultDir)

      displayResult(spark, queryNum, querySummary)
  }

  def createTableAndRunAllQueries(spark: SparkSession, tables: Array[String], resultDir: String = tpcdsWorkDir): Unit = {
    import spark.implicits._

    createTables(spark, tables)

    spark.sql(s"USE ${tpcdsDatabaseName}")

    val allSummary = runAllQueries(spark, resultDir)

    displaySummary(allSummary)
  }
}

object SparkRunner{

  def main(args: Array[String]) {

    val spark = SparkSession.
        builder().
        config("spark.ui.showConsoleProgress", false).
        config("spark.sql.autoBroadcastJoinThreshold", -1).
        config("spark.sql.crossJoin.enabled", true).
        getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    // TPC-DS table names.
    val tables = Array("call_center", "catalog_sales",
                      "customer_demographics", "income_band",
                      "promotion", "store", "time_dim", "web_returns",
                      "catalog_page", "customer", "date_dim",
                      "inventory", "reason", "store_returns", "warehouse",
                      "web_sales", "catalog_returns", "customer_address",
                      "household_demographics", "item", "ship_mode", "store_sales",
                      "web_page", "web_site" )
    
    if (args.length == 0) {
      println("No arguments specified")
      sys.exit(1)
    }

    val arglist = args.toList

    val tpcdsdir = arglist(0)
    val tpcdsdb = arglist(1)

    println(s"using database ${tpcdsdb}; data stored in ${tpcdsdir}/data")
    val benchmark = new TPCDSBenchmark(tpcdsdir, tpcdsdb)
    
    arglist(2) match {
      case "1" => benchmark.createTables(spark, tables)
      case "2" => benchmark.createTableAndRunIndividualQuery(spark, tables, arglist(3).toInt)
      case "3" => benchmark.createTableAndRunAllQueries(spark, tables)
      case "4" => benchmark.displaySummary(benchmark.runIndividualQuery(spark, arglist(3).toInt))
      case "5" => benchmark.displaySummary(benchmark.runAllQueries(spark))
    }

    spark.stop()
  }
}