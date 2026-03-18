// Get rosetta table info
println("ROSETTA TABLES:")
val tables = spark.sql("SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='mi_team' AND table_name LIKE 'rosetta_%' ORDER BY table_name DESC").collect()
for (row <- tables) {
  println(s"  ${row.getString(0)}")
}

println("\nLatest rosetta table schema:")
if (tables.length > 0) {
  val latest = tables(0).getString(0)
  println(s"Table: $latest")
  spark.sql(s"SELECT * FROM mi_team.$latest LIMIT 0").printSchema()
  val count = spark.sql(s"SELECT COUNT(*) as cnt FROM mi_team.$latest").collect()(0).getLong(0)
  println(s"Rows: $count")
}
