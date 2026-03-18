# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## This table only has to be created once a year at the beginning of June!  Use the first ani, topic cluster mapping, and citation tables created in June of the given year.  the analysis will be run over last_year-6 to last_year-1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Note: the 2024 data has been generated on 20250509 and will need to be regenerated in June when the new topic-cluster mapping from SciVal is available.
# MAGIC
# MAGIC ## Georgios Giatrakis - 20250612
# MAGIC I had to ask help from my colleagues for this question and the answer is “This file will be generated only when there are any updates to topics (ex: new topics included). We don't expect any changes this year.”
# MAGIC
# MAGIC If you have any follow up questions, I cc Ganesh and Elena who have more knowledge in this area.

# COMMAND ----------

# MAGIC %md
# MAGIC # Includes

# COMMAND ----------

from pyspark.sql import functions as func
from pyspark.sql import Window
from pyspark.sql.types import *
from datetime import datetime
strNow = datetime.strftime(datetime.now(),"%Y%m%d")
strNowMonth = strNow[0:6]

# COMMAND ----------

# MAGIC %md
# MAGIC # Settings

# COMMAND ----------

save_output = True
display_output = True

# RB20250509 shifted both by a year
analyze_year = 2024
bottom_year = 2019

# Scopus snapshot date
scopus_snapshot_date = '20250601'

# Source date
ops_edc_source_date = '20250609'
# dbutils.fs.ls('/mnt/els/edc/source-profiles-parsed-edc/')

# Usage data date
usage_date = '20250601'

# output path
path_output = '/mnt/els/rads-mappings/burst_analysis/topics/' + str(analyze_year) + '/'

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

def load_dataframe(df_input_path):
  return (
    spark
    .read
    .format("parquet")
    .load(df_input_path)
  )

# COMMAND ----------

def save_dataframe(df_output,df_output_path,partitions,save_output,display_output):
  if save_output:
    print('Saving output to: ' + df_output_path)
    (
      df_output
      .repartition(partitions)
      .write
      .mode("overwrite")
      .format("parquet")
      .save(df_output_path)
    )
    print('Reading result from: ' + df_output_path + ' (limited to 10)')
    if display_output:
      display(
        spark
        .read
        .format("parquet")
        .load(df_output_path)
        # limit to 10 to keep tables manageable
        .limit(10)
      )
  else:
    print('Displaying results only (limited to 10)')
    if display_output:
      # limit to 10 to keep tables manageable
      display(df_output.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Find dates of Eid to Topic mapping and topic-to-topic cluster mapping

# COMMAND ----------

a = dbutils.fs.ls('/mnt/els/com-elsevier-scival-tech-data/eid-topic-mapping/')

x = [a[i][0] for i in range(len(a))]

newesttop = max(x)
print(newesttop)

# COMMAND ----------

b = dbutils.fs.ls('/mnt/els/com-elsevier-scival-tech-data/topic-to-topic-cluster-mapping/') 

#take the most recent topic to cluster mapping as this updated far less frequently.
y = [b[i][0] for i in range(len(b))]

#get the full date
newestcluster = max(y)

if newestcluster[-11:-1] > newesttop[-11:-1]:
  while newestcluster[-11:-1] > newesttop[-11:-1]:
    y.remove(max(y))
    newestcluster = max(y)
  relclustermap = newestcluster
else:
  relclustermap = newestcluster
  
print(relclustermap)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load EDC Source Data (for CiteScore)

# COMMAND ----------

df_ops_edc_sources = load_dataframe('/mnt/els/edc/source-profiles-parsed-edc/' + ops_edc_source_date)

df_ops_edc_sources_cleaned = (
  df_ops_edc_sources
  # no need to explode to filter the metrics_calulations on year
  .withColumn('calculations',func.explode(func.expr('filter(calculations, x -> x.year between ' + str(bottom_year) + ' and ' + str(analyze_year) + ')'))) #[0]
  # # we use the citescore of all document types
  .withColumn("citescore", func.col("calculations.csMetric.csCiteScore").cast('double'))
  # # no need for journals with no citescore.
  .filter('citescore IS NOT NULL')
  # add year
  .withColumn("year", func.col("calculations.year"))
  .select(
    func.col('id').alias('srcid'),
    func.col('year').alias('data_year'),
    'citescore',
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load usage data

# COMMAND ----------

df_usage = spark.read.parquet('/mnt/els/rads-main/mappings_and_metrics/bibliometrics/publication_level/snapshot_metrics/' + usage_date + '/Usage_Data')

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Topic metrics

# COMMAND ----------

df_topic_clusters = (
  sqlContext.read.option('delimiter','|').csv(relclustermap+"/topic-to-topic-cluster-mapping.csv")
  .withColumnRenamed("_c0","TopicID")
  .withColumnRenamed("_c1","Cluster")
)

# display(df_topic_clusters)

# COMMAND ----------

decay=0.8

# to get a weighted average from the time series, we use the decay.
decay_weights =[decay**a for a in range(analyze_year-bottom_year,0,-1)]

df_decay = spark.createDataFrame([[decay_weights]],['dw'])

# make a dataframe which will be used ON THE LEFT to collect data such as views, citations, and citescore.
# this way we won't miss any years should there be a null (however unlikely) when making frames out of pubs. We
# can then use the fillna function to put 0's into these places.


df_years = spark.createDataFrame([[y] for y in range(bottom_year,analyze_year+1)],['data_year'])

# bring in the desired ani snapshot
df_ani_sv =(
  table("ops_etl.ani_" + scopus_snapshot_date)
  .select(
    "Eid",
    "source.srcid",
    "source.type",
    "citation_type",
    "datesort",
    "source.date_month",
    "source.date_day",
    "source.date_year",
    "citations"
  )
  .filter("citation_type is null or citation_type not in ('pp','er')")
  .withColumn("datesortFormatted",func.date_format(func.to_date(func.col("datesort"), "yyyyMMdd"), "yyyy-MM-dd"))
  .withColumn("date_month",  func.when((func.isnull(func.col("date_month")))|(func.col("date_month") == 0), func.lit("01")).otherwise(func.col("date_month")))
  .withColumn("date_day",  func.when((func.isnull(func.col("date_day"))) | (func.col("date_day") == 0), func.lit("01")).otherwise(func.col("date_day")))
  .withColumn("datesort",func.when((func.isnull(func.col("date_year")))  | (func.col("date_year") == 0), func.col("datesortFormatted") ).otherwise(func.concat(func.col("date_year"), func.lit("-"), func.col("date_month"), func.lit("-"),  func.col("date_day"))))
  .withColumn("sort_year_2", func.substring(func.col("datesort"), 1, 4))
  .drop("date_month", "date_day", "date_year", "datesortFormatted")
  .select(
    "Eid",
    "sort_year_2",
    "citations",
    "srcid"
  )
  .filter("sort_year_2 >= 1996")  
)
#bring in the most recent mapping of topics to topic clusters

df_topic = (
  sqlContext.read.csv(newesttop+'eid-topic-mapping.csv',header=False )
  .withColumnRenamed('_c0','TopicID')
  .withColumnRenamed('_c1','Eid')
  .withColumn('TopicID',func.col("TopicID").cast(LongType()))
  .withColumn('Eid',func.regexp_extract('Eid', '2-s2.0-([0-9]+)',1).cast(LongType()))
)
  
#display(df_years)

# COMMAND ----------

# publications by topic and sort year

df_pubs = (
  df_ani_sv
  .where(func.col("sort_year_2").between(bottom_year,analyze_year))
  .select("Eid",func.col("sort_year_2").alias("data_year"))
  .join(df_topic,"Eid","left_outer")
  .select("Topicid","data_Year","Eid")
  .distinct()
  .groupBy("Topicid","data_Year")
  .agg(
    func.count("Eid").alias("Output")
  )
)

df_views = (
    df_usage
    .withColumnRenamed("usage_year","data_year")
    .join(df_ani_sv.where(func.col("sort_year_2").between(bottom_year,analyze_year)).select("Eid","sort_year_2"),"Eid","left_outer")
    .join(df_topic,"Eid","left_outer")
    #.join(df_topichl,"topicid_hl","left_outer")
    .select("TopicId","data_Year","Eid","abstract_views","outward_links","sort_year_2")
    .filter("data_year between sort_year_2 and sort_year_2+1")
    .groupBy("TopicId","data_year")
    .agg(
      (func.sum("abstract_views")+func.sum("outward_links")).alias("views")
    )
)

df_citescore = (
  df_ani_sv.select("Eid","srcid",func.col("sort_year_2").alias("data_year"))
  .where(func.col("sort_year_2").between(bottom_year,analyze_year))
  .join(df_topic,"Eid")
  #.join(df_topichl,"topicid_hl","left_outer")
  .join(df_ops_edc_sources_cleaned,["srcid","data_year"],"left_outer")
  .select("TopicID","Data_Year","citescore","Eid")
  #.filter("citescore is not null")
  .groupBy("TopicID","Data_Year")
  .agg(
    func.avg("citescore").alias("citescore"),
  )
  .fillna(0,"citescore")
)

# df_citations =(
#    df_citations_by_year
#   .join(df_ani.where(func.col("sort_year").between(bottom_year,analyze_year)).select("Eid"),"Eid")
#   .join(df_topic,"Eid","left_outer")
#   #.join(df_topichl,"topicid_hl","left_outer")
#   .select("Eid","sort_year","TopicID",func.col("cite_year").alias("data_year"),"citations")
#   .distinct()
#   .filter("data_year between sort_year and sort_year+1")
#     .groupBy("TopicID","data_year")
#     .agg(
#       func.sum("citations").alias("citations")
#     )
# )

df_citations_2 =(
  df_ani_sv
  .where(func.col("sort_year_2").between(bottom_year,analyze_year))
  .select(func.col("Eid").alias("citing"),func.col("sort_year_2").alias("data_year"),func.explode_outer("citations").alias("Eid"))
  .join(df_ani_sv.select("Eid","sort_year_2"),"Eid","left")
  .join(df_topic,"Eid","left")
  .distinct()
  .filter("data_year between sort_year_2 and sort_year_2+1")
  .groupBy("TopicID","data_year")
    .agg(
      func.count("citing").alias("citations")
    )
)

#display(df_citescore.filter("topicid=78954"))

# COMMAND ----------

df_cit_avg = (
    df_citations_2
    .groupBy("data_year")
    .agg(
        func.avg(func.log1p("citations"))
    )
)
# display(df_cit_avg)

# COMMAND ----------

df_all_info =(
  df_years
  .crossJoin(
    df_topic
    .select("TopicID")
    .distinct()
  )
)

# COMMAND ----------

#create a udf to make a dictionary using the years and values for citations, views, and cite score

def mapmaker(x,y,z):
  p ={}
  for j in x:
    if type(j[z]) is type(None):
      u = {j[y]:None}
      p.update(u)
    else:
      
      u={j[y]: 1.0*j[z]}
      p.update(u)
  return p

mapper = func.udf(mapmaker,MapType(LongType(),FloatType()))

# COMMAND ----------

df_topic_info =(
  df_all_info
  .join(df_pubs,["TopicID","data_year"],"left_outer")
   .join(df_views,["TopicID","data_year"],"left_outer")
   .join(df_citations_2,["TopicID","data_year"],"left_outer")
   .join(df_citescore,["TopicID","data_year"],"left_outer")
  .fillna(0,["output","views","citations","citescore"])
)

# df_info =(
#   df_topic_info
#   .crossJoin(df_years)
#   .select("year","topicid","data_year","output","citations","citescore","views")
#   .fillna(0,["output","citations","citescore","views"])
#   .groupBy("TopicID","year")
#   .agg(
#     func.collect_list(func.struct("data_year","output","citations","views","citescore")).alias("data")
#   )
#   .withColumn("pubs_year",mapper("data",func.lit("data_year"),func.lit("output")))
#   .withColumn("cs_year",mapper("data",func.lit("data_year"),func.lit("citescore")))
#   .withColumn("views_year",mapper("data",func.lit("data_year"),func.lit("views")))
#   .withColumn("cites_year",mapper("data",func.lit("data_year"),func.lit("citations")))
#   .withColumn("Cj",func.log1p(
#     func.expr("(cites_year[year])"))
#              )
#   .withColumn("Vj",func.log1p(func.expr("(views_year[year])")))
#   .withColumn("CSj",func.log1p(func.expr("cs_year[year]")))
#   .select("TopicID","year","data","Cj","Vj","CSj")
#   .distinct()
# )

# COMMAND ----------

df_info = (
  df_topic_info
  .select(
    "topicid",
    "data_year",
    "output",
    "citations",
    "citescore",
    "views"
  )
  .withColumn("Cj",func.log1p(func.col("citations")))
  .withColumn("Vj",func.log1p(func.col("views")))
  .withColumn("CSj",func.log1p(func.col("citescore")))
  .select("TopicID","data_year","output","citations","Views","Citescore","Cj","Vj","CSj")
  .distinct()
)

# display(df_info.filter("topicid = 93404 or topicid = 96121"))

# COMMAND ----------

df_info_mean =(
  df_info
  .groupBy("data_year")
  .agg(
    func.avg("Cj").alias("avgCj"),
    func.avg("Vj").alias("avgVj"),
    func.avg("CSj").alias("avgCSj"),
    func.stddev("Cj").alias("stdCj"),
    func.stddev("Vj").alias("stdVj"),
    func.stddev("CSj").alias("stdCSj")
  )
)

# display(df_info_mean)

# COMMAND ----------

df_topic_prom =(
  df_info
  .join(df_info_mean,"data_year","left_outer")
  .withColumn("Prominence",0.495*(func.col("Cj")-func.col("avgCj"))/func.col("stdCj") + 0.391*(func.col("Vj")-func.col("avgVj"))/func.col("stdVj") + 0.114*(func.col("CSj")-func.col("avgCSj"))/func.col("stdCSj"))
  .withColumn("prom_min",func.min("prominence").over(Window.partitionBy("data_year")))
  .withColumn("Rank",func.rank().over(Window.partitionBy("data_year").orderBy(func.desc("prominence"),func.desc("output"),func.asc("topicid"))))
  # Do not exclude topics
  #.withColumn("TopicCount",func.expr("sum(case when output > 0 then 1 else 0 end) over(partition by data_year)"))
  #.withColumn("TopicCountall",func.expr("count(topicid) over(partition by data_year)"))
  #.withColumn('ProminencePerc',func.expr('(1-((Rank-1)/TopicCount))*100'))
  .withColumn('ProminencePerc',func.expr('((max(rank) over (partition by data_year))-Rank+1)/(max(rank) over (partition by data_year))*100'))
)

# display(df_topic_prom.filter("(topicid = 93404 or topicid = 96121 or topicid = 71863 or topicid = 71867) and data_year = 2021"))

# COMMAND ----------

import numpy as np

def Burst(s):
  try:
    obs = s[len(s)-1]
    vlist = s[0:len(s)-1]
    avg=np.average(vlist,weights=decay_weights)
    variance = float(np.average((vlist-avg)**2, weights=decay_weights))
    std=np.sqrt(variance)
    score=(obs - avg)/std
    if ((std == 0) & (obs!=avg)):
    # we have all equal values and a different observation
    # if we do not add the obs to the list, we get the odd situation 
    # that with no variance but a different obs, the score is infinity. 
    # That doesnt help anyone, so we add obs with a low weight, to ensure we get a non-zero std
      ext_vlist=vlist+[obs]
      ext_decay_weights=decay_weights+[decay**(len(decay_weights)+1)]
      avg=np.average(ext_vlist,weights=ext_decay_weights)
      variance = float(np.average((ext_vlist-avg)**2, weights=ext_decay_weights))
      std=np.sqrt(variance)
      score=(obs - avg) * float("infinity")
    if std != 0: 
       score=(obs - avg) / std
    if ((variance==0) & (obs==avg)):
       score=0
    else:
       score=(obs - avg) / std
    if ((variance==0) & (obs==avg)):
        score=0
    return type('obj', (object,), {'obs':float(obs),'average' : float(avg),'variance':variance,'burstScore':float(score),'std':float(std)})
  
  except:
    return(None)


schemaFazscore = StructType([
  StructField('obs',FloatType(),False),
  StructField("average", FloatType(), False),
  StructField("variance", FloatType(), False),
  StructField("burstScore", FloatType(), False),
  StructField("std", FloatType(), False)
])

def split(s):
  obs = s[len(s)-1]
  vlist = s[0:len(s)-1]
  print(str(obs)+'     '+str(vlist)   )

BurstScoreAnalysis = func.udf(Burst,schemaFazscore)
x= [1,2,3,4,5,6]
split(x)

# COMMAND ----------

# RB20250509 it doesn't look like this is being used anywhere

df_burst_1 =(
  df_topic_prom
  .groupBy("TopicID")
  .agg(
    func.collect_list(func.struct("data_year","output","Prominence","ProminencePerc",func.col("Rank").alias("ProminenceRank"))).alias("data")
  )
  .withColumn("data",func.array_sort("data"))
#   .withColumn("Output_by_year",mapper("data",func.lit("year"),func.lit("output")))
#   .withColumn("Prominence_by_year",mapper("data",func.lit("year"),func.lit("Prominence")))
#   .withColumn("PromPerc_by_year",mapper("data",func.lit("year"),func.lit("ProminencePerc")))
#   .withColumn("Prom_Ordered",func.map_values(func.col("Prominence_by_year").sort(func.asc("year"))))
  .withColumn("Prominence_Ordered",func.col("data.Prominence"))
  .withColumn("Prom_Ordered_Len",func.size("Prominence_Ordered"))
  .sort(func.asc("Prom_Ordered_Len"))
)

# display(df_burst_1)

# COMMAND ----------

df_burst_2 = (
  df_topic_prom
  .groupBy("TopicID")
  .agg(
    func.collect_list(
      func.struct(
        "data_year",
        "output",
        "Prominence",
        "ProminencePerc",
        func.col("Rank").alias("ProminenceRank")
      )
    ).alias("data")
  )
  .withColumn("data",func.array_sort("data"))
  .withColumn("Output_by_year",mapper("data",func.lit("data_year"),func.lit("output")))
  .withColumn("Prominence_by_year",mapper("data",func.lit("data_year"),func.lit("Prominence")))
  .withColumn("Prominence_Rank_by_Year",mapper("data",func.lit("data_year"),func.lit("ProminenceRank")))
  .withColumn("PromPerc_by_year",mapper("data",func.lit("data_year"),func.lit("ProminencePerc")))
  .withColumn("Prominence_ordered",func.col("data.Prominence"))
  .withColumn("Output_ordered",func.col("data.output"))
  .withColumn("Burst_Stats_Prominence",BurstScoreAnalysis("Prominence_Ordered"))
  .withColumn("Burst_Stats_Output",BurstScoreAnalysis("Output_Ordered"))
  .drop("data")
)

# display(df_burst_2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Burst 2

# COMMAND ----------

save_dataframe(df_burst_2,path_output,200,save_output,display_output)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load generated data

# COMMAND ----------

df_burst_2_loaded = load_dataframe(path_output)

display(df_burst_2_loaded.filter("topicid = 3070"))

# COMMAND ----------

print('All done :)')