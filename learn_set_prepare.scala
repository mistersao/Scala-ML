import java.text.SimpleDateFormat

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.getOrCreate
import spark.implicits._

def ever30Udf (as: String) = {
if (List("4", "5", "6", "7", "8", "9", "7") contains as) 1 else 0
}

def getColsWithPeriod() = {
  val PERIOD_CODES = Array("behave_all_1_3_7", "behave_all_1_3_8", "behave_all_1_3_9", "behave_all_1_3_10", "behave_all_1_3_11", "behave_all_1_3_12","behave_all_1_4_1", "behave_all_1_4_2", "behave_all_1_4_3", "behave_all_1_4_4", "behave_all_1_4_5", "behave_all_1_4_6")
  array(PERIOD_CODES.map(col): _*)
}


def choose_criteria = udf((
   as:Seq[String] 
) => {
if(as.map(x => {
  if(x != null) x.length
  else 0
}).exists(x => x > 0)) 1
else 0
})

def sumUdf = udf((as: Seq[String]) => {
  as.map(value => if (value == null || value.length > 10 || value.contains("null") || value.contains("-9223372036854630879")) 0 else value.toInt).sum
})

/*
def default_criteria = udf ((a: String, b: String) => {
if (a == null && b == null) -1
else if ((a != null && a.length() != 0 && ever30Udf(a) == 1) ||(b != null && b.length() != 0 && ever30Udf(b) == 1)) 1
else if ((a != null && a.length() != 0 && a.contains('1')) || (b != null && b.length() != 0 &&b.contains('1'))) 0
else 2
})
*/

def default_criteria = udf ((a: String, b: String, c: String, d: String, e: String, f: String) => {
if (a == null && b == null && c == null && d == null && e == null && f == null) 2
else if (ever30Udf(a) == 1 || ever30Udf(d) == 1) 1
else if (((a!= null && a.contains('1')) || (a != null && a.length()!= 0 && b!= null && b.contains('1')) || (a != null && a.length()!= 0 && b != null && b.length()!= 0 && c!= null && c.contains('1'))) ||
((d!= null && d.contains('1')) || (d != null && d.length()!= 0 && e!= null && e.contains('1')) || (d != null && d.length()!= 0 && e != null && e.length()!= 0 && f!= null && f.contains('1')))) 0
else 2
})


val fl="file:///..."
val fn="file:///..."
val fk="file:///..."
val fg="file:///..."
val fz="file:///..."
val fx="file:///..."
val fm="file:///..."


val srl = spark.read.format("csv").option("sep", ";").option("header", "true").load(fl)
val srn = spark.read.format("csv").option("sep", ";").option("header", "true").load(fn)
val srk = spark.read.format("csv").option("sep", ";").option("header", "true").load(fk)
val srg = spark.read.format("csv").option("sep", ";").option("header", "true").load(fg)
val srz = spark.read.format("csv").option("sep", ";").option("header", "true").load(fz)
val srx = spark.read.format("csv").option("sep", ";").option("header", "true").load(fx)
val srm = spark.read.format("csv").option("sep", ";").option("header", "true").load(fm)


val src = srn.union(srk).union(srg).union(srz).union(srx).union(srm)

val columns = src.columns.filter(c => c.contains("RULE_")) ++ src.columns.filter(c => c.contains("behave_all_1_3_")) ++ src.columns.filter(c => c.contains("behave_all_1_4_")) ++ src.columns.filter(x => x.contains("sum_amt_")).filter(x => !x.contains("00")) 
val columns1 = srl.columns.filter(c => c.contains("RULE_")) ++ srl.columns.filter(c => c.contains("behave_all_1_3_")) ++ srl.columns.filter(c => c.contains("behave_all_1_4_")) ++ srl.columns.filter(x => x.contains("sum_amt_")).filter(x => !x.contains("00")) 


//src.withColumn("data",$=col("eventDate").cast("date")).filter("data >= '2019-07-01' AND data <= '2019-07-31'").count

val src_out = src.filter("category_state == 'regular'").select(columns.head, columns.tail: _*
).withColumn("sum_amt_wo_closed", sumUdf(array(
$"sum_amt_same_01_mortgage",
$"sum_amt_same_01_auto",
$"sum_amt_same_01_creditCard",
$"sum_amt_same_01_consumerCredit",
$"sum_amt_same_01_il_16",
$"sum_amt_same_01_pdl_16",
$"sum_amt_same_11_mortgage",
$"sum_amt_same_11_auto",
$"sum_amt_same_11_creditCard",
$"sum_amt_same_11_consumerCredit",
$"sum_amt_same_11_il_16",
$"sum_amt_same_11_pdl_16",
$"sum_amt_same_10_mortgage",
$"sum_amt_same_10_auto",
$"sum_amt_same_10_creditCard",
$"sum_amt_same_10_consumerCredit",
$"sum_amt_same_10_il_16",
$"sum_amt_same_10_pdl_16",
$"sum_amt_outer_01_mortgage",
$"sum_amt_outer_01_auto",
$"sum_amt_outer_01_creditCard",
$"sum_amt_outer_01_consumerCredit",
$"sum_amt_outer_01_il_16",
$"sum_amt_outer_01_pdl_16",
$"sum_amt_outer_11_mortgage",
$"sum_amt_outer_11_auto",
$"sum_amt_outer_11_creditCard",
$"sum_amt_outer_11_consumerCredit",
$"sum_amt_outer_11_il_16",
$"sum_amt_outer_11_pdl_16",
$"sum_amt_outer_10_mortgage",
$"sum_amt_outer_10_auto",
$"sum_amt_outer_10_creditCard",
$"sum_amt_outer_10_consumerCredit",
$"sum_amt_outer_10_il_16",
$"sum_amt_outer_10_pdl_16"))
).withColumn("chosen_criteria", choose_criteria(getColsWithPeriod())
).withColumn("default", default_criteria ($"behave_all_1_3_12", $"behave_all_1_3_9", $"behave_all_1_3_8", $"behave_all_1_4_6", $"behave_all_1_4_3", $"behave_all_1_4_2"))


val src_out1 = srl.filter("category_state == 'regular'").select(columns1.head, columns1.tail: _*
).withColumn("sum_amt_wo_closed", sumUdf(array(
$"sum_amt_same_01_mortgage",
$"sum_amt_same_01_auto",
$"sum_amt_same_01_creditCard",
$"sum_amt_same_01_consumerCredit",
$"sum_amt_same_01_il_16",
$"sum_amt_same_01_pdl_16",
$"sum_amt_same_11_mortgage",
$"sum_amt_same_11_auto",
$"sum_amt_same_11_creditCard",
$"sum_amt_same_11_consumerCredit",
$"sum_amt_same_11_il_16",
$"sum_amt_same_11_pdl_16",
$"sum_amt_same_10_mortgage",
$"sum_amt_same_10_auto",
$"sum_amt_same_10_creditCard",
$"sum_amt_same_10_consumerCredit",
$"sum_amt_same_10_il_16",
$"sum_amt_same_10_pdl_16",
$"sum_amt_outer_01_mortgage",
$"sum_amt_outer_01_auto",
$"sum_amt_outer_01_creditCard",
$"sum_amt_outer_01_consumerCredit",
$"sum_amt_outer_01_il_16",
$"sum_amt_outer_01_pdl_16",
$"sum_amt_outer_11_mortgage",
$"sum_amt_outer_11_auto",
$"sum_amt_outer_11_creditCard",
$"sum_amt_outer_11_consumerCredit",
$"sum_amt_outer_11_il_16",
$"sum_amt_outer_11_pdl_16",
$"sum_amt_outer_10_mortgage",
$"sum_amt_outer_10_auto",
$"sum_amt_outer_10_creditCard",
$"sum_amt_outer_10_consumerCredit",
$"sum_amt_outer_10_il_16",
$"sum_amt_outer_10_pdl_16"))
).withColumn("chosen_criteria", choose_criteria(getColsWithPeriod())
).withColumn("default", default_criteria ($"behave_all_1_3_12", $"behave_all_1_3_9", $"behave_all_1_3_8", $"behave_all_1_4_6", $"behave_all_1_4_3", $"behave_all_1_4_2"))


val src_out_total = src_out.union(src_out1)

//src_out.select($"behave_all_1_3_10",$"behave_all_1_4_4",$"default").show(20)

//src_out.filter("chosen_criteria == 1 AND default == 0").count
//Long = 96140
//Long = 97254
//Long = 97662


//src_out.filter("chosen_criteria == 1 AND default == 1").count
//Long = 7995
//Long = 5474
//Long = 1875


//src_out.filter("chosen_criteria == 1 AND default == 2").count
//Long = 21120
//Long = 22527
//Long = 25718


src_out_total.filter("chosen_criteria == 1").coalesce(1).write.option("header", "true").option("sep", ";").mode("append").csv("file:///...")