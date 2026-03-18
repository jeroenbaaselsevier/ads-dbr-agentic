# Databricks notebook source
# MAGIC %md # init parameters

# COMMAND ----------

from pyspark.sql import functions as func

# COMMAND ----------

sort_pub_year='sort_year'
minyear='1960' # only affects the npY1Y3 column.
mincityear='1996'
maxcityear='2024' # only including citations up to this year.
maxyear='2024'
minyear_overall='1788' # this should usually be 1788, this is used for the lower bound filter for citation stats, otherwise includes citations to papers < minyear.
includepp=False
min_count_arcpre='5' # how many ar/cp/re/ in career to be included.
min_npY1Y3='2' # is 2 by default.
doctypes=None

list_supress_authors=[]

def manual_table1_fixes(df):
    return (
        df
        .withColumn(
            'inst_name',
            func
            .when(
                func.col('author_id').isin([7203030601,7401866412,7004328455,7004744483]),
                func.lit('Stanford University')
            )
            .when(
                func.col('author_id').isin(list_supress_authors),
                func.lit('<redacted>')
            )
            .otherwise(func.col('inst_name'))
        )
        .withColumn(
            'authfull',
            func
            .when(
                func.col('author_id').isin(list_supress_authors),
                func.lit('<redacted>')
            )
            .otherwise(func.col('authfull'))
        )
    )

# COMMAND ----------

ani_stamp='20250801'
ipr_stamp='20250801'
apr_stamp='20250801'
sm_mapping_date="20250801"


# COMMAND ----------

# where to store our intermediate tables and results
basePath_project='dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/'

# COMMAND ----------

df_ani=(
  table(f"scopus.ani_{ani_stamp}")
)

df_ipr=(
  table(f"scopus.ipr_{ipr_stamp}")
)

df_apr=(
  table(f"scopus.apr_{apr_stamp}")
)

df_smc_mapping_labels=spark.read.format("delta").load('dbfs:/mnt/els/rads-main/legacy_sm_hw/rads_pscopus/classification')  

# science-metrix hybrid classification
df_smc=(
  spark
  .read
  .format("delta")
  .load(f"dbfs:/mnt/els/rads-main/legacy_sm_hw/rads_pscopus/sm_classification_eid_complete_mapping_{sm_mapping_date}")
  .withColumn('subfield_match',func.lower(func.trim(func.col('subfield_hybrid'))))
  .join(df_smc_mapping_labels.withColumn('subfield_match',func.lower(func.trim(func.col('subfield')))),['subfield_match'])
  .select(
    'Eid','Domain','Field','Subfield'
  )
)


# COMMAND ----------

# MAGIC %md ## discontinued sources

# COMMAND ----------

# discontinued titles Jul 2025 (using the list used for THE WUR 26)
discontinued_sources=[16395, 21100316027, 21100787766, 21100408192, 11000153760, 12446, 21101085290, 24805, 19700187706, 21101023074, 21100840026, 22621, 21100898760, 21100248002, 19700190354, 21100898023, 19900191928, 21100902642, 64325, 10600153357, 17600155134, 19700174627, 21101068164, 21100943319, 21566, 18082, 21100873480, 19700201144, 21100840445, 19700175055, 19700182690, 19216, 21100216519, 16838, 20341, 21100854119, 19700175833, 21100411340, 25975, 144613, 19700200708, 19700186824, 18000156707, 4700152772, 17300154704, 21100200601, 144676, 21795, 21101039058, 21101052926, 16606, 4700151607, 4900153302, 13221, 21100828027, 21100806998, 4700153606, 19700175283, 13900154722, 3900148509, 5100152904, 4000151703, 21100205743, 21100792740, 30049, 17717, 144885, 15458, 19900192202, 20200195004, 21101079121, 12997, 19600164600, 145357, 21101119543, 21100199344, 20980, 17700155030, 5700191222, 19900192209, 21100241217, 21100398858, 21100316064, 21100896881, 17700156513, 20600195618, 19700173002, 21100236616, 5700164382, 12000154492, 21101039068, 19900192173, 19700175175, 11300153738, 19700200724, 19900191960, 16400154778, 25900, 18060, 17700155011, 16402, 19700182104, 21101140508, 21100201076, 21100894504, 20145, 19700175121, 21101061447, 21100199850, 21100215707, 21101133327, 1000147113, 19900191924, 21101021990, 21100313913, 21100229162, 21101151614, 5700155185, 10600153363, 11300153315, 4600151508, 27490, 21100195303, 21100197942, 21100855502, 22018, 144942, 21101045037, 21100890307, 20500195433, 19900192586, 21100890290, 21100945713, 4700152483, 21100837986, 26700, 19700175302, 17266, 21100860009, 21100201522, 3900148202, 21100399172, 21100201055, 28531, 19700167903, 17606, 19700188355, 4000151604, 21101039054, 13800154702, 13398, 21100417465, 16300154755, 14148, 7200153152, 21100940522, 19700186829, 21100241608, 19700175270, 19700175122, 18800156745, 5100155058, 21100908447, 5700165153, 19700182042, 21100317750, 18551, 19335, 24221, 27819, 21100819610, 21100217234, 19700188407, 21101089961, 17644, 21496, 21100244835, 19700182218, 28513, 19926, 19700188348, 20500195139, 16987, 21100318417, 15500154707, 21100884991, 23650, 21100301405, 21100223146, 7700153105, 21100824403, 21100198713, 28195, 21100856144, 19700201308, 19700175143, 19400158329, 130151, 65906, 19700181106, 21100782386, 16084, 12100157101, 19900191611, 21100211302, 19201, 18800156718, 19700194018, 21100913565, 17400154823, 4700152479, 23413, 18626, 19700175036, 19700177128, 12986, 20180, 5400152617, 22172, 3900148201, 17600155049, 22219, 19400158357, 21100970313, 21100204506, 20500195215, 21287, 25312, 18500166400, 5400152704, 21100199803, 20511, 21101019739, 5700165154, 21100831810, 39264, 21101038826, 21100197714, 21000196006, 63434, 21101041557, 21100944136, 21100201525, 7400153105, 21100784750, 4500151521, 19700201471, 21100377768, 18300156718, 19700175758, 21101023717, 4600151522, 50205, 28633, 5600153105, 17700155408, 21100889409, 17284, 21100435543, 19700188428, 21101074754, 13600154710, 16300154705, 80618, 21000195625, 21100228084, 27958, 21100495829, 21100805731, 21100198464, 4000151807, 21100223585, 5700170931, 16500154707, 17895, 78090, 19700201521, 19182, 6500153240, 21100298063, 4700152608, 21101050917, 19700181240, 21100936125, 21100889873, 23824, 4400151521, 14268, 23886, 11500153515, 19900191946, 21100897507, 21100828961, 21100197765, 21100374810, 24343, 37673, 12930, 17675, 21100902608, 21100225606, 16228, 15300154804, 21101029728, 65449, 21100913341, 20000195080, 21100898670, 4400151723, 29729, 19274, 21100235609, 21100329555, 19700175174, 28594, 16390, 19700176236, 19900192206, 19900192203, 19900193502, 20631, 21100204113, 100147021, 21100855996, 19700201333, 21100399105, 21100782416, 5600152865, 19700174998, 21100868092, 20000195053, 26946, 21100798510, 21100435271, 21100204304, 19399, 19700186885, 8300153132, 21100228316, 20533, 29201, 18225, 21101048271, 19872, 19700201139, 21101196737, 19700188444, 15600154708, 21101044895, 21100854010, 21101052764, 19700174988, 21100201065, 17700156008, 18800156705, 21100904334, 19700175106, 22137, 21100825150, 15687, 19700175083, 4400151716, 19900191942, 19300157108, 12100156721, 13884, 14598, 21101012520, 21100232418, 19900192210, 14332, 21100829147, 21100899502, 21100228751, 10400153308, 12300154715, 19900191965, 13600154732, 21100913891, 19700176302, 6400153142, 11100153313, 19700188435, 19900192207, 17700156220, 76272, 21416, 19700188317, 10600153337, 19632, 19300157107, 19900191923, 19700200831, 19600157004, 28739, 21100901133, 21100286923, 17600155110, 13154, 20500195146, 18300156727, 5800179590, 21100806906, 19700187642, 21100896634, 21100814505, 19700174933, 31872, 5400152620, 21100791821, 144793, 22049, 21100855407, 21100805732, 19700175137, 19364, 21100239831, 17900156726, 19700174645, 19700175060, 21101052847, 19700188326, 17600155122, 21100903068, 145018, 21101039171, 19900193211, 9500154041, 11300153601, 3400148107, 19700188324, 16556, 19400157128, 70245, 19700200860, 27374, 21100283701, 11100153312, 21100381259, 19700174907, 19900192601, 20500195414, 21100855844, 11200153306, 21100899004, 24847, 21100896316, 70864, 19700174931, 17700156404, 21100944442, 21100871693, 22756, 29764, 89410, 13770, 22998, 11300153722, 19700174987, 5700161108, 4700151914, 23632, 20400195007, 19700175177, 23587, 19917, 17600155138, 21100904912, 21100231630, 19700173325, 19700186910, 19300157028, 21100356015, 14714, 21100237602, 21719, 21101038576, 14984, 19700188484, 52429, 5800198357, 23823, 19700175151, 7000153240, 15400155900, 21100799500, 21100894501, 84320, 19700175031, 98396, 29851, 15196, 22749, 21100244802, 21100244634, 13100154702, 21100202909, 21100231100, 21100825369, 17543, 19700182619, 21100863113, 19700174893, 19900191916, 24498, 18800156724, 38581, 30411, 19900192205, 19900191939, 19700187801, 21101111528, 21100920227, 21100817618, 21211, 18412, 17300154715, 21100850746, 19700201140, 25253, 19700175008, 28109, 19700175829, 19700174810, 14770, 15500154702, 19700186852, 21100829275, 21100873483, 21100200821, 14671, 14111, 11900154394, 21100256101, 19700188420, 88997, 21100216333, 19700174971, 21100944103, 19700188318, 21100902545, 21100875478, 21100332206, 144842, 28046, 28657, 21100197912, 21100246541, 19700175828, 16550, 24051, 21100872051, 22392, 21100197523, 3200147807, 21100870214, 71491, 19700175176, 6400153122, 21101093332, 19700174617, 15356, 38536, 21101072199, 15642, 13223, 21100838044, 21100244847, 19700188309, 21100237401, 21100237426, 21100244805, 21100926589, 4800152402, 23141, 21100322426, 10900153329, 21100199307, 17100154710, 21100468968, 17600155114, 19700171709, 12100156333, 7200153143, 21100521165, 17700155031, 21100278103, 24244, 6400153128, 19700174914, 21100332454, 7200153130, 21100903490, 12400154721, 3900148513, 21100781704, 18280, 4700152475, 19300157018, 21100218545, 21101079125, 18800156704, 21100283772, 16061, 19900191926, 21100890303, 17700156323, 21100415050, 24065, 21100415047, 145526, 19700174813, 19700181206, 19900193524, 19700201516, 21100886224, 21100200832, 21100942112, 21100212114, 19700174647, 19700174967, 22891, 87584, 19900191927, 17133, 19700174950, 21101128483, 21100945711, 25476, 21101045285, 21100329542, 21101016504, 21100201982, 15314, 20804, 21100334845, 21100944119, 19700175045, 21100201709, 130094, 4500151502, 21101017732, 21100201518, 21100790061, 19600166307, 21760, 11500153415, 20000195017, 20100195016, 21100202936, 21100199112, 21101044916, 21083, 21100201062, 19700174900, 21101170720, 21101068178, 21100863640, 21100808402, 19700174653, 21100944441, 21100830706, 19700182031, 17500155122, 4800152306, 12009, 63518, 91247, 100147321, 19700173022, 19700186825, 11200153302, 21100408176, 21100856538, 19700188319, 19700186827, 19900193646, 21100199822, 21100943519, 12100154839, 19600164100, 22476, 28618, 21100197967, 1900147401, 21100367736, 51117, 17700156205, 19600161809, 19700175161, 12300154727, 17800156756, 5100152606, 4400151502, 21100408983, 6100153027, 28546, 25166, 21100936532, 21100265336, 21100983110, 21101016918, 21101038744, 82170, 25197, 7900153132, 28720, 5000157006, 21100297818, 21100846309, 21101056815, 29782, 19700188422, 19700167901, 18100156703, 21100887430, 5700165212, 17600155009, 19500156802, 20159, 20458, 19400158817, 21100223579, 23344, 18584, 28043, 21100293201, 14046, 11600154151, 15784, 21101037290, 17600155047, 4000148801, 19700175101, 19700174979, 21100905326, 21101039169, 5200152617, 4700152857, 21100301603, 18500168200, 21100787020, 14243, 16338, 17100154711, 21100889429, 11200153556, 19700177302, 21100201971, 21100896268, 21100264003, 19700174801, 4100151710, 19900191919, 19800188067, 21100241786, 19700201509, 19200156706, 21101049548, 11700154724, 21100205702, 19700200853, 21100983356, 14500154705, 16755, 21100265048, 21100886412, 28515, 4700151906, 5400152637, 6300153113, 21248, 21100869510, 21100212600, 19900192208, 17200154704, 25299, 19203, 21100422125, 19700173025, 6700153288, 21101104815, 17900156733, 17500155017, 32824, 21000195011, 17600155123, 20400195018, 21100208309, 17700156720, 21100943317, 12313, 71472, 21100316045, 21101152618, 21101034437, 13600154725, 21100925870, 65672, 19700171018, 21100373959, 20125, 20100195054, 18665, 21000195021, 19700169710, 21101021773, 21100794004, 21100877663, 21101085289, 21100268407, 21100223710, 21100429502, 21100205709, 22525, 17614, 19700188206, 15683, 24741, 19700166519, 20500195412, 21100247092, 4700152769, 21100821129, 21100915633, 19900191347, 21100856014, 21100437958, 21800, 21100785495, 19700175778, 21100230500, 19381, 6000195383, 16500154705, 19700175064, 16693, 11700154613, 19978, 11300153309, 29444, 13585, 18800156725, 21100255395, 21100855999, 19300157035, 21100854867, 21100857169, 16500154706, 21100446518, 19700175035, 21101024217, 21101065325, 25980, 26562, 20600195619, 19700175066, 5700161261, 16505, 19700174941, 22725, 21100790713, 14000156160, 21100877173, 21101089994, 21101042009, 15252, 12324, 19900193655, 21100773742, 145515, 110124, 17200154703, 50077, 19900191925, 21100317903, 18500159400, 20826, 17700155411, 19700176044, 21101098814, 19700175858, 21100928820, 23806, 19700173245, 66263, 21100836263, 21100887701, 21100469375, 16127, 21100874917, 9800153152, 21100781400, 21100867942, 17700154923, 21101037901, 17700156703, 21100373226, 24557, 23020, 21100810604, 19400157518, 21100389315, 19600157349, 17700156005, 19700174730]

# COMMAND ----------

# MAGIC %md # author override assignments

# COMMAND ----------

eid_author_old_new=[
    [3042845898,7102614881,
]
]

# COMMAND ----------

# MAGIC %md # run base

# COMMAND ----------

# ## clear caches after RWDB approach change:
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/cache/20240801/temp_df_ani_rw',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/cache/20240801/temp_df_agg_count_1960_1996_2023_1788_ppFalse_minarcpre_5_maxcityear_2023',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/cache/20240801/temp_df_agg_count_ln_max_1960_1996_2023_1788_ppFalse_minarcpre_5_maxcityear_2023',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/cache/20240801/temp_df_agg_count_ln_wmaxc_1960_1996_2023_1788_ppFalse_minarcpre_5_maxcityear_2023',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/Table-S1_20240801_1960_1996_2023_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2023.parquet',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/Table-S1_20240801_1960_1996_2023_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2023_top2p_bysubfield_100K_combined.csv',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/excel_export/career_2023_pubs_since_1788_wopp_extracted_202408/Table_1_Authors_career_2023_pubs_since_1788_wopp_extracted_202408.xlsx',True)

# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/cache/20240801/temp_df_agg_count_1960_2023_2023_1788_ppFalse_minarcpre_5_maxcityear_2023',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/cache/20240801/temp_df_agg_count_ln_max_1960_2023_2023_1788_ppFalse_minarcpre_5_maxcityear_2023',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/cache/20240801/temp_df_agg_count_ln_wmaxc_1960_2023_2023_1788_ppFalse_minarcpre_5_maxcityear_2023',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/Table-S1_20240801_1960_2023_2023_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2023.parquet',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/Table-S1_20240801_1960_2023_2023_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2023_top2p_bysubfield_100K_combined.csv',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20240801/excel_export/career_2023_pubs_since_1788_wopp_extracted_202408/Table_1_Authors_singleyr_2023_pubs_since_1788_wopp_extracted_202408.xlsx',True)



# COMMAND ----------

# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/temp_df_apr_auth_name_inst',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/Table-S1_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024.parquet', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/Table-S3_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024_SM_FIELD.csv', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_grouped_SM_FIELD_1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_window_SM_FIELD__1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_window_SM_FIELD_toplist__1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_grouped_TOTAL_1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_window_TOTAL__1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_window_TOTAL_toplist__1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/Table-S3_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024_SM_SUBFIELD.csv', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_grouped_SM_SUBFIELD_1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_window_SM_SUBFIELD__1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# # dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/TS3_window_SM_SUBFIELD_toplist__1960_1996_2024_1788_ppFalse_minarcpre_5_maxcityear_2024_minnpY1Y3_2', True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/Table-S1_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024_top2p_bysubfield_100K_combined.csv', True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/cache/20250801/temp_df_apr_auth_name_inst',True)
# dbutils.fs.rm('dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/Table-S1_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024.parquet',True)

# COMMAND ----------

# MAGIC %run "./top cited scholars - base v10"

# COMMAND ----------

export_filename_pattern=('career' if mincityear=='1996' else 'singleyr' if mincityear==maxyear else mincityear )+'_'+maxyear+Y4_name_postfix+('_wpp' if includepp else '_wopp')+'_extracted_'+ani_stamp[0:6]
print(export_filename_pattern)

# COMMAND ----------

print(csvOutFileName_s1+'_top2p_bysubfield_100K_combined.csv')

# COMMAND ----------

# MAGIC %md # output

# COMMAND ----------

# MAGIC %md ## Table 1: all authors

# COMMAND ----------

df_table1_ranked_author_selection=(
  spark.read.csv(csvOutFileName_s1+'_top2p_bysubfield_100K_combined.csv',header = 'true', quote='"', escape='"')
  .orderBy(func.col('rank (ns)').cast('long').asc())
)

# COMMAND ----------

print(csvOutFileName_s1+'_top2p_bysubfield_100K_combined.csv')

# COMMAND ----------

print(csvOutFileName_s3+'_SM_SUBFIELD.csv')
print(csvOutFileName_s3+'_SM_FIELD.csv')

# COMMAND ----------

# MAGIC %md ## Table 2a: subfield aggregates

# COMMAND ----------

# DBTITLE 0,Table S8a and S8b
df_table_2_subfield=(
  spark.read.csv(csvOutFileName_s3+'_SM_SUBFIELD.csv',header = 'true', quote='"', escape='"')
  .withColumnRenamed('subject_column','subfield')
  .join(df_smc_mapping_labels,['subfield'],'LEFT_OUTER')
  .withColumn('subfield',func.coalesce('subfield',func.lit("Unassigned")))
  .orderBy(func.expr("IFNULL(domain,'ZZ')").asc(),func.asc("field"),func.expr("IF(subfield='TOTAL','ZZ',subfield)").asc())
  .select(
    func.col('domain').alias('Domain'),func.col('field').alias('Field'),func.col('subfield').alias('Subfield'),
    func.col('Auth').alias('#Auth'),
    func.col('Auth-top-100k-ns').alias('#Auth top 100k (ns)'),
    func.expr('CONCAT(ROUND((`Auth-top-100k-ns`/`Auth`)*100,2),"%")').alias('% in 100k (ns)'),
    func.col('Auth-top-100k').alias('#Auth top 100k'),
    func.expr('CONCAT(ROUND((`Auth-top-100k`/`Auth`)*100,2),"%")').alias('% in 100k'),
    func.col('Auth-in-top-list').alias('#Auth in top-list'),
    func.expr('CONCAT(ROUND((`Auth-in-top-list`/`Auth`)*100,2),"%")').alias('% in top-list'),
    func.col('Cites-25').alias('Cites@25'),
    func.col('Cites-50').alias('Cites@50'),
    func.col('Cites-75').alias('Cites@75'),
    func.col('Cites-90').alias('Cites@90'),
    func.col('Cites-95').alias('Cites@95'),
    func.col('Cites-99').alias('Cites@99'),
    func.round('c-25',3).alias('c@25'),
    func.round('c-50',3).alias('c@50'),
    func.round('c-75',3).alias('c@75'),
    func.round('c-90',3).alias('c@90'),
    func.round('c-95',3).alias('c@95'),
    func.round('c-99',3).alias('c@99'),

        # the citation stats across the entire population in the higher bands are affected by the long tail of researchers with low publication volumes
    # i.e. the self citation cutoff percentage @95 percentile = 100% self cites, because of a large group with low citation volume (of which the chance
    # of self-cites is higher)
    # therefore omitting the following columns from the results:
#     func.col('selfp-95').alias('self%@95'),
#     func.col('selfp-99').alias('self%@99'),
#     func.col('cprat-95').alias('cprat@95'),
#     func.col('cprat-99').alias('cprat@99'),
#     func.col('cprat-ns-95').alias('cprat@95 (ns)'),
#     func.col('cprat-ns-99').alias('cprat@99 (ns)'),
    
    func.col('top-list-selfp-95').alias('top-list self%@95'),
    func.col('top-list-selfp-99').alias('top-list self%@99'),
    func.col('top-list-cprat-95').alias('top-list cprat@95'),
    func.col('top-list-cprat-99').alias('top-list cprat@99'),
    func.col('top-list-cprat-ns-95').alias('top-list cprat@95 (ns)'),
    func.col('top-list-cprat-ns-99').alias('top-list cprat@99 (ns)'),
    
    # % of authors from the top-list in the top-list based top percentiles is not very meaningful. 
    # it will yield the percentile most of the time, i.e. 95th percentile = 5 percent of the authors, exception of course are ties.
    # omitting this from the result table.
#     func.expr('CONCAT(ROUND((`Auth-in-top-list-selfp-95`/`Auth-in-top-list`)*100,2),"%")').alias('% in top-list in self%@95'), 
  )
)

# COMMAND ----------

# MAGIC %md ## Table 2b: field aggregates

# COMMAND ----------

df_table_2_field=(
  spark.read.csv(csvOutFileName_s3+'_SM_FIELD.csv',header = 'true', quote='"', escape='"')
  .withColumnRenamed('subject_column','field')
  .join(df_smc_mapping_labels.select('domain',func.trim('field').alias('field')).distinct(),['field'],'LEFT_OUTER')
  .withColumn('field',func.coalesce('field',func.lit("Unassigned")))
  .orderBy(func.expr("IFNULL(domain,'ZZ')").asc(),func.expr("IF(Field='TOTAL','ZZ',Field)").asc())
  
  .select(
    func.col('domain').alias('Domain'),func.col('field').alias('Field'),
    func.col('Auth').alias('#Auth'),
    func.col('Auth-top-100k-ns').alias('#Auth top 100k (ns)'),
    func.expr('CONCAT(ROUND((`Auth-top-100k-ns`/`Auth`)*100,2),"%")').alias('% in 100k (ns)'),
    func.col('Auth-top-100k').alias('#Auth top 100k'),
    func.expr('CONCAT(ROUND((`Auth-top-100k`/`Auth`)*100,2),"%")').alias('% in 100k'),
    func.col('Auth-in-top-list').alias('#Auth in top-list'),
    func.expr('CONCAT(ROUND((`Auth-in-top-list`/`Auth`)*100,2),"%")').alias('% in top-list'),
    func.col('Cites-25').alias('Cites@25'),
    func.col('Cites-50').alias('Cites@50'),
    func.col('Cites-75').alias('Cites@75'),
    func.col('Cites-90').alias('Cites@90'),
    func.col('Cites-95').alias('Cites@95'),
    func.col('Cites-99').alias('Cites@99'),
    func.round('c-25',3).alias('c@25'),
    func.round('c-50',3).alias('c@50'),
    func.round('c-75',3).alias('c@75'),
    func.round('c-90',3).alias('c@90'),
    func.round('c-95',3).alias('c@95'),
    func.round('c-99',3).alias('c@99'),
    
    # the citation stats across the entire population in the higher bands are affected by the long tail of researchers with low publication volumes
    # i.e. the self citation cutoff percentage @95 percentile = 100% self cites, because of a large group with low citation volume (of which the chance
    # of self-cites is higher)
    # therefore omitting the following columns from the results:
#     func.col('selfp-95').alias('self%@95'),
#     func.col('selfp-99').alias('self%@99'),
#     func.col('cprat-95').alias('cprat@95'),
#     func.col('cprat-99').alias('cprat@99'),
#     func.col('cprat-ns-95').alias('cprat@95 (ns)'),
#     func.col('cprat-ns-99').alias('cprat@99 (ns)'),
    
    func.col('top-list-selfp-95').alias('top-list self%@95'),
    func.col('top-list-selfp-99').alias('top-list self%@99'),
    func.col('top-list-cprat-95').alias('top-list cprat@95'),
    func.col('top-list-cprat-99').alias('top-list cprat@99'),
    func.col('top-list-cprat-ns-95').alias('top-list cprat@95 (ns)'),
    func.col('top-list-cprat-ns-99').alias('top-list cprat@99 (ns)'),
    
    # % of authors from the top-list in the top-list based top percentiles is not very meaningful. 
    # it will yield the percentile most of the time, i.e. 95th percentile = 5 percent of the authors, exception of course are ties.
    # omitting this from the result table.
#     func.expr('CONCAT(ROUND((`Auth-in-top-list-selfp-95`/`Auth-in-top-list`)*100,2),"%")').alias('% in top-list in self%@95'),  
  )
  
)


# COMMAND ----------

# MAGIC %md ## dataset stats

# COMMAND ----------

# [x1] % of the scientists who are in the top-2% of their subdiscipline for career-long impact when self-citations are included are no longer be in the top-2% of their subdiscipline when self-citations are excluded
# [x2] % of them fall below the top 10%
# Of the 158,932 top-cited scientists of table S6 classified by a subdiscipline, [x3] have a ratio of citations over citing papers exceeding the 99th percentile for their subdiscipline
display(
  df_agg_result_parquet
  .withColumn('top_listed',func.expr(top_list_expression))
  .join(windowedTableS3(df_agg_result_parquet,func.col('sm_subfield_1'),'SM_SUBFIELD').withColumnRenamed('subject_column','sm_subfield_1'),['sm_subfield_1'],'LEFT_OUTER')
  .agg(
    func.count('*').alias('totalcount'),
    func.count(func.expr('IF(top_listed,TRUE,NULL)')).alias('toplist_count'),
    func.count(func.expr('IF(top_listed,sm_subfield_1,NULL)')).alias('toplist_count_w_subfield'),
    func.count(func.expr('IF((rank_sm_subfield_1_ws/count_sm_subfield_1 <=.02),TRUE,NULL)')).alias('auths_top2p_ws'),
    func.count(func.expr('IF((rank_sm_subfield_1_ws/count_sm_subfield_1 <=.02) AND (rank_sm_subfield_1_ns/count_sm_subfield_1 >.02),count_sm_subfield_1,NULL)')).alias('auths_top2p_ws_not_ns'),
    func.count(func.expr('IF((rank_sm_subfield_1_ws/count_sm_subfield_1 <=.02) AND (rank_sm_subfield_1_ns/count_sm_subfield_1 >.1),count_sm_subfield_1,NULL)')).alias('auths_top2p_ws_not_top_10p_ns'),
    func.count(func.expr('IF(ws_cprat>=`top-list-cprat-99`,IF(top_listed,ws_cprat,NULL),NULL)')).alias('x3_cprat_above_p99_top-list'),
    func.count(func.expr('IF(top_listed,IF(sm_subfield_1 IS NULL, NULL, ws_cprat),NULL)')).alias('toplist_count_w_subfield_w_ws_cprat'),
  )
  .withColumn('x1_fraction_2p_ws_2p_ns_dropout',func.expr('auths_top2p_ws_not_ns/auths_top2p_ws'))
  .withColumn('x2_fraction_2p_ws_10p_ns_dropout',func.expr('auths_top2p_ws_not_top_10p_ns/auths_top2p_ws'))
)

# COMMAND ----------

# MAGIC %md ## self-rate dist

# COMMAND ----------

# display(
#   df_agg_result_parquet
#   .withColumn('subfield_rec_count',func.when(func.col('sm_subfield_1').isNull(),func.lit(None)).otherwise(func.count('*').over(Window.partitionBy('sm_subfield_1'))))
#   .withColumn('cprat_perc',func.when(func.col('sm_subfield_1').isNull()|func.col('ws_cprat').isNull(),func.lit(None)).otherwise(func.rank().over(Window.partitionBy('sm_subfield_1').orderBy(func.asc('ws_cprat')))/func.col('subfield_rec_count')))
#   .withColumn('top_listed',func.expr(top_list_expression))
#   .filter('sm_subfield_1="Nanoscience & Nanotechnology"')
#   #.filter('top_listed')
#   .filter('ws_cprat<4')
#   .select('author_id','ws_cprat','cprat_perc','ws_ncY2Y3','ws_ncY2Y3_cp','top_listed')
# )

# COMMAND ----------

# MAGIC %md ## Table 3: Max values
# MAGIC To assist self-calculating a c-score.

# COMMAND ----------

df_table_3_maxvalues=(
  get_lnmaxvalues_csv()
  .select(
    *[
      func.col('ws_maxl'+i[0]).alias(i[1]) for i in [['nc',f'nc{Y2}{Y3}'],['h',f'h{Y3}'],['hm',f'hm{Y3}'],['ns','ncs'],['nsf','ncsf'],['nsfl','ncsfl']]
    ]+[
      func.col('ns_maxl'+i[0]).alias(i[1]+" (ns)") for i in [['nc',f'nc{Y2}{Y3}'],['h',f'h{Y3}'],['hm',f'hm{Y3}'],['ns','ncs'],['nsf','ncsf'],['nsfl','ncsfl']]
    ]
  )
)

# COMMAND ----------

# MAGIC %md # Excel export

# COMMAND ----------

# MAGIC %md ## Table 1: ranked author list

# COMMAND ----------

url=gen_workbook_table1(f'Table_1_Authors_{export_filename_pattern}',manual_table1_fixes(df_table1_ranked_author_selection),key_data)
print('download result:')
print(url)


# COMMAND ----------

# MAGIC %md ## Tables 2: field/subfield

# COMMAND ----------

# download link:
url=gen_workbook_table2_field_subfield(f'Table_2_field_subfield_thresholds_{export_filename_pattern}',df_table_2_field,df_table_2_subfield,data_cols_width_styles_table2_field,data_cols_width_styles_table2_subfield)
print('download result:')
print(url)

# COMMAND ----------

# MAGIC %md ## Table 3

# COMMAND ----------

# download link:
url=gen_workbook_table3_maxvals(f'Table_3_maxlog_{export_filename_pattern}',data_cols_width_styles_table3)
print('download result:')
print(url)

# COMMAND ----------

# MAGIC %md # _internal_ share exported files

# COMMAND ----------

from dataframe_functions import share_file_path
share_file_path(
    os.path.join(basePath_project.replace('dbfs:','').replace('/mnt/els/','s3://'),'excel_export',export_filename_pattern),
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('testUser'),
    "cited_scholars_"+export_filename_pattern
)