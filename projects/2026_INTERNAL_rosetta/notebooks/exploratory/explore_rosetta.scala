// Explore rosetta table structure and snapshots
println("\n" + "="*80)
println("EXPLORING ROSETTA TABLE")
println("="*80)

// Try to list all tables matching rosetta pattern
try {
  println("\n1. Looking for rosetta tables using information_schema...")
  val result = spark.sql("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_name LIKE '%rosetta%'
    ORDER BY table_schema, table_name
  """).collect()
  
  if (result.length > 0) {
    println(s"\nFound ${result.length} rosetta tables:")
    result.foreach(row => {
      println(s"  ${row.getAs[String](0)}.${row.getAs[String](1)}")
    })
    
    // Get the latest one
    val latest = result.last
    val fullName = s"${latest.getAs[String](0)}.${latest.getAs[String](1)}"
    println(s"\nInspecting latest: $fullName")
    
    val df = spark.table(fullName)
    println(s"\nSchema:")
    df.printSchema()
    println(s"\nRow count: ${df.count()}")
    println(s"\nFirst 3 rows:")
    df.show(3, false)
  } else {
    println("No rosetta tables found in information_schema")
  }
} catch {
  case e: Exception => println(s"Error with information_schema: ${e.getMessage}")
}

// Alternative: try direct mi_team access
try {
  println("\n2. Trying direct mi_team database access...")
  val tables = spark.sql("SHOW TABLES IN mi_team").collect()
  val rosettaTables = tables.filter(t => t.getAs[String](1).toLowerCase.contains("rosetta")).map(_.getAs[String](1))
  
  if (rosettaTables.length > 0) {
    println(s"Found rosetta tables via SHOW TABLES: ${rosettaTables.mkString(", ")}")
    val latest = rosettaTables.sorted.last
    val df = spark.table(s"mi_team.$latest")
    println(s"\nSchema of $latest:")
    df.printSchema()
    println(s"Row count: ${df.count()}")
    println(s"First 3 rows:")
    df.show(3, false)
  } else {
    val allTables = tables.map(_.getAs[String](1)).take(10)
    println(s"No rosetta tables in mi_team. Available tables (first 10): ${allTables.mkString(", ")}")
  }
} catch {
  case e: Exception => println(s"Error with mi_team: ${e.getMessage}")
}

println("\n" + "="*80)
