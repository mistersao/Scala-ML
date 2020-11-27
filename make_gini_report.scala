// processing result of gini calculation
// collected all data in one file
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.getOrCreate
import org.apache.spark.sql.functions._


val home_path="file:///..."

val default_defs=Array(
/*  "behave_1_4_3_ever30"
  , "behave_1_4_market_moment_ever30"
  ,*/ "behave_1_4_market_gross_ever30"
/*  , "behave_1_4_12_ever30"
  , "behave_1_4_market_moment_ever30"
  , "behave_1_4_market_gross_ever30"
  , "behave_1_4_3_ever0"
  *///, "behave_status_ever30"
//  , "behave_1_4_market_moment_ever0"
//  , "behave_1_4_market_gross_ever0"*/
)

//val clientSubType = "SEMI_PRIMARY"
//val clientSubType = "FULL_PRIMARY"

val rpt_mcs=Array(
    "LV01FF_32"
  // , "HY01LL_32"
  // , "SB01BB_32"
  ,"XV01BB_32"
  , "YU01LL_32"
  ,  "4K01FF_32"
  , "BK01FF_32"
  , "B701FF_32"
  //,"1V01RR_32"
  //,"8S01GG_32"
)

val rpt_vars=Array(
  "new_delinq_6_1_bin"
  //,"curBalance_total_binned"
  //"segment_binned"
  //"segment_mfo_bank_year_month_week_bin_semi"
  //"segment_mfo_bank_year_bin_full"
  //"segment_mfo_bank_year_bin_semi"
  /*
  "segment_reject_mfo_week"
  , "segment_reject_mfo_month"
  , "segment_reject_mfo_quater"
  , "segment_reject_mfo_year"
  ,"segment_mfo_bank_week"
  ,"segment_mfo_bank_month"
  ,"segment_mfo_bank_quater"
  ,"segment_mfo_bank_year"*/
  /*,$"segment_reject_bank_week"
  , $"segment_reject_bank_month"
  , $"segment_reject_bank_quater"
  , $"segment_reject_bank_year"
  "vadim_reject_var"
  , "score_new_v1"
  , "v_delinq_v2_2_pp"
  , "rjb_2_IL_pp"
  , "v1_bin1_pp"
  , "v0_bin1_pp"
  , "limit_bin_pp"
  , "score"
  , "score_SC_011_002"*/
)

val rpt_dts=Array(
  /*"201904"
,  "201905"
,  "201906"
,  "201908"
 ,  "201907"
 ,  */"201909"
  , "201910"
  ,  "201911"
  ,  "201912"
)

def report(default_def: String, mc: String, var1: String, dt: String, home_path: String)={
spark.read.format("csv").option("sep", ";"
).option("header", "true"
).load(home_path+mc+"/"+"/Validation/Regular/"+/*clientSubType*/ "ALL_PRIMARY" +"/"+default_def+"/"+dt+"/gini_tables/"+var1+"/"
//).load(home_path+mc+"/"+"/Validation/regular/"+default_def+"/"+dt+"/gini_tables/"+var1+"/"
).withColumn("default_def",lit(default_def)
).withColumn("mc",lit(mc)
).withColumn("var1",lit(var1)
).withColumn("YM",lit(dt)
)
}

val res=default_defs.map( default_def => rpt_mcs.map( mc => rpt_vars.map( var1 => rpt_dts.map( dt => report(default_def, mc, var1, dt, home_path) ).reduce(_ union _) ).reduce(_ union _) ).reduce(_ union _) ).reduce(_ union _)
res.coalesce(1).write.option("header", "true").option("sep", ";").mode("append").csv(home_path + "gini_svod/")// uncomment in order to save in file
res.show(100)// uncomment on order to show at screen