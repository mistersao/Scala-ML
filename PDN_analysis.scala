import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


val spark = SparkSession.builder.getOrCreate

val sc = SparkContext.getOrCreate

import spark.implicits._

val mc = "8S01GG_32"

val period = "201909"

//FUNCTIONS FROM PREPROCESSOR.SCALA
def ever30Udf = udf((as: String) => {
  if (List("2", "3", "4", "5", "6", "7", "8", "9", "7", "Z") contains as) 1 else 0
})

def filterByType(clientType: String) = {
  if (clientType == "PRIMARY") " AND (count_same_00 + count_same_01 + count_same_10 + count_same_11) == 0"
  else if (clientType == "REPEAT") " AND (count_same_00 + count_same_01 + count_same_10 + count_same_11) > 0"
  else ""
}

def filterBySubType(clientSubType: String) = {
  if (clientSubType == "SEMI_PRIMARY") " AND (`V23a_@16` + `V23b_@16` + `V24_@16` + `V23_@16`) > 0"
  else if (clientSubType == "FULL_PRIMARY") " AND (`V23a_@16` + `V23b_@16` + `V24_@16` + `V23_@16`) == 0"
  else ""
}

def behave_1_4_market_gross_udf =udf((eventPaymPat: String, hor: Integer) => {
  var pat = eventPaymPat
  if ((pat.length() > 0) & (eventPaymPat(eventPaymPat.length() - 1) == '0')) pat = pat.substring(0, pat.length() - 1)
  if (pat.length() > 0) {
    val rightCount = Math.min(hor, pat.length())
    pat = if (rightCount > pat.length()) pat else pat.substring(pat.length() - rightCount)
    val order = "X01BAC23457Z89"
    val max = if (pat.isEmpty) -1 else pat.map(order indexOf _).max
    if (max == -1) Char.MinValue.toString
    else order.charAt(max).toString
  }
  else "X"
})


def false_true = udf((a: String) => {
  a.toDouble > 0
}
)

def sumUdf = udf((as: Seq[String]) => {
  as.map(value => if (value.length > 10 || value.contains("null") || value.contains("-9223372036854630879")) 0 else value.toInt).sum
})

//

//FUNCTIONS FROM VALIDATOR_ALL.SCALA
val clientType = "PRIMARY"
//val clientType = "REPEAT"

//val clientSubType = "SEMI_PRIMARY"
//val clientSubType = "FULL_PRIMARY"

//
/////////////////////////////////CREDIT CARD
val maxTermsAmt_out_CreditCard_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_CreditCard_01@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxTermsAmt_out_CreditCard_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_CreditCard_10@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxTermsAmt_out_CreditCard_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_CreditCard_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxTermsAmt_out_CreditCard_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_CreditCard_00@" + u + "_" + "1"
}).map(c => col(c)): _*)



//

val curBalanceAmt_out_CreditCard_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_CreditCard_01@" + u + "_" + "1"
}).map(c => col(c)): _*)

val curBalanceAmt_out_CreditCard_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_CreditCard_10@" + u + "_" + "1"
}).map(c => col(c)): _*)

val curBalanceAmt_out_CreditCard_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_CreditCard_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val curBalanceAmt_out_CreditCard_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_CreditCard_00@" + u + "_" + "1"
}).map(c => col(c)): _*)

//



val maxAmtPastDue_out_CreditCard_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_CreditCard_01@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_CreditCard_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_CreditCard_10@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_CreditCard_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_CreditCard_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_CreditCard_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_CreditCard_00@" + u + "_" + "1"
}).map(c => col(c)): _*)

//////////////////////////////////////MICRO


val maxTermsAmt_out_micro_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_micro_01@" + u + "_" + "1"
}).map(c => col(c)): _*)


val maxTermsAmt_out_micro_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_micro_10@" + u + "_" + "1"
}).map(c => col(c)): _*)


val maxTermsAmt_out_micro_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_micro_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxTermsAmt_out_micro_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_micro_00@" + u + "_" + "1"
}).map(c => col(c)): _*)

//

val curBalanceAmt_out_micro_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_micro_01@" + u + "_" + "1"
}).map(c => col(c)): _*)


val curBalanceAmt_out_micro_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_micro_10@" + u + "_" + "1"
}).map(c => col(c)): _*)


val curBalanceAmt_out_micro_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_micro_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val curBalanceAmt_out_micro_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_micro_00@" + u + "_" + "1"
}).map(c => col(c)): _*)

//

val maxAmtPastDue_out_micro_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_micro_01@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_micro_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_micro_10@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_micro_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_micro_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_micro_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_micro_00@" + u + "_" + "1"
}).map(c => col(c)): _*)



///////////////////////////////////////////////169


val maxTermsAmt_out_169_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_169_01@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxTermsAmt_out_169_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_169_10@" + u + "_" + "1"
}).map(c => col(c)): _*)


val maxTermsAmt_out_169_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_169_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxTermsAmt_out_169_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxTermsAmt_out_169_00@" + u + "_" + "1"
}).map(c => col(c)): _*)

//

val curBalanceAmt_out_169_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_169_01@" + u + "_" + "1"
}).map(c => col(c)): _*)

val curBalanceAmt_out_169_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_169_10@" + u + "_" + "1"
}).map(c => col(c)): _*)


val curBalanceAmt_out_169_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_169_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val curBalanceAmt_out_169_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "curBalanceAmt_out_169_00@" + u + "_" + "1"
}).map(c => col(c)): _*)

//

val maxAmtPastDue_out_169_01_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_169_01@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_169_10_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_169_10@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_169_11_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_169_11@" + u + "_" + "1"
}).map(c => col(c)): _*)

val maxAmtPastDue_out_169_00_year = array((for (u <- Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
) yield {
  "maxAmtPastDue_out_169_00@" + u + "_" + "1"
}).map(c => col(c)): _*)

//ANALYTICS

def fullPrimarySegment = udf((isCurrentdelinqNon16: Boolean, v1: Double, sum_V23: Double)=>{
if (isCurrentdelinqNon16 == true) "with_past_due"
else if (isCurrentdelinqNon16 == false && v1 == 0) "no_past_due_no_opened_with_closed"
else if (isCurrentdelinqNon16 == false && v1 > 0 && sum_V23 == 0) "no_past_due_with_opened_no_closed"
else if (isCurrentdelinqNon16 == false && v1 > 0 && sum_V23 > 0) "no_past_due_with_opened_with_closed"
else "no_segment"
})

def curBalance_12m_bin = udf((sum_curBalance_total: Int)=>{
  val a = sum_curBalance_total
  if (a == 0) 1
  else if (a > 0 && a <= 5000) 2
  else if (a > 5000 && a <= 15000) 3
  else if (a > 15000 && a <= 30000) 4
  else if (a > 30000 && a <= 70000) 5
  else if (a > 70000) 6
  else 7
})

val file_name = "B:/" + period + "/" + mc

val output_path = "file:///.../"+mc+"/"+clientType+"/"/*+clientSubType*/

val src = spark.read.format("csv").option("sep", ";").option("header", "true").load("file:///" + file_name)
val columns = Array("fid", "serialNum", "eventDate", "event_param_num2", "paymtPat") ++ src.columns.filter(c => c.contains("maxTermsAmt_out") || c.contains("curBalanceAmt_out") || c.contains("maxAmtPastDue_out") || c.contains("amtOutstanding_out") || c.contains("sum_amt_outer") || c.contains("V23") || c.contains("V24") || c.contains("v1"))
//src.select(columns.head, columns.tail: _*)
val src_out = src.filter("model_code == 'SC_011_002' AND category_state == 'regular' AND (count_00 + count_01 + count_10 + count_11) > 0 AND event_param_num2 < 30000" + filterByType(clientType) /*+ filterBySubType(clientSubType)*/
).select(columns.head, columns.tail: _*
).withColumn("sum_amt_outer_16_sum", sumUdf(array(
  $"sum_amt_outer_01_il_16"
  , $"sum_amt_outer_01_pdl_16"
  , $"sum_amt_outer_10_il_16"
  , $"sum_amt_outer_10_pdl_16"
  , $"sum_amt_outer_11_il_16"
  , $"sum_amt_outer_11_pdl_16"))
).withColumn("sum_amt_outer_1_mortgage_sum", sumUdf(array(
  $"sum_amt_outer_01_mortgage"
  , $"sum_amt_outer_10_mortgage"
  , $"sum_amt_outer_11_mortgage"))
).withColumn("sum_amt_outer_1_auto_sum", sumUdf(array(
  $"sum_amt_outer_01_auto"
  , $"sum_amt_outer_10_auto"
  , $"sum_amt_outer_11_auto"))
).withColumn("sum_amt_outer_1_creditCard_sum", sumUdf(array(
  $"sum_amt_outer_01_creditCard"
  , $"sum_amt_outer_10_creditCard"
  , $"sum_amt_outer_11_creditCard"))
).withColumn("sum_amt_outer_1_consumerCredit_sum", sumUdf(array(
  $"sum_amt_outer_01_consumerCredit"
  , $"sum_amt_outer_10_consumerCredit"
  , $"sum_amt_outer_11_consumerCredit"))
).withColumn("v1_1", $"V23a_@1" + $"V23b_@1" + $"V24_@1"
).withColumn("v1_6", $"V23a_@6" + $"V23b_@6" + $"V24_@6"
).withColumn("v1_7", $"V23a_@7" + $"V23b_@7" + $"V24_@7"
).withColumn("v1_9", $"V23a_@9" + $"V23b_@9" + $"V24_@9"
).withColumn("v1_16", $"V23a_@16" + $"V23b_@16" + $"V24_@16"
).withColumn("v_16", $"v1_16" + $"V23_@16"
).withColumn("v1", $"v1_1" + $"v1_6" + $"v1_7" + $"v1_9" + $"v1_16"
).withColumn("sum_V23", $"V23_@1"+$"V23_@7"+$"V23_@9"+$"V23_@6"
).withColumn("sum_amt_outer_all_sum", ($"sum_amt_outer_16_sum" + $"sum_amt_outer_1_mortgage_sum" + $"sum_amt_outer_1_auto_sum" + $"sum_amt_outer_1_creditCard_sum" + $"sum_amt_outer_1_consumerCredit_sum")
).withColumn("sum_amt_outer_non16_sum", ($"sum_amt_outer_all_sum" - $"sum_amt_outer_16_sum")
).withColumn("isCurrentdelinqNon16", false_true($"sum_amt_outer_non16_sum")
).withColumn("full_primary_segment", fullPrimarySegment($"isCurrentdelinqNon16",$"v1",$"sum_V23")
).withColumn("behave_1_4_market_gross", behave_1_4_market_gross_udf($"paymtPat", lit(3))
).withColumn("behave_1_4_market_gross_ever30", ever30Udf($"behave_1_4_market_gross")
).withColumn("behave_1_4_market_gross_ever30", ever30Udf($"behave_1_4_market_gross")
).withColumn("behave_1_4_market_gross_ever30", ever30Udf($"behave_1_4_market_gross")
).withColumn("sum_curBalance_CreditCard",sumUdf(curBalanceAmt_out_CreditCard_01_year) + sumUdf(curBalanceAmt_out_CreditCard_10_year) + sumUdf(curBalanceAmt_out_CreditCard_11_year) + sumUdf(curBalanceAmt_out_CreditCard_00_year)
).withColumn("sum_curBalance_micro",sumUdf(curBalanceAmt_out_micro_01_year) + sumUdf(curBalanceAmt_out_micro_10_year) + sumUdf(curBalanceAmt_out_micro_11_year) + sumUdf(curBalanceAmt_out_micro_00_year)
).withColumn("sum_curBalance_169",sumUdf(curBalanceAmt_out_169_01_year) + sumUdf(curBalanceAmt_out_169_10_year) + sumUdf(curBalanceAmt_out_169_11_year) + sumUdf(curBalanceAmt_out_169_00_year)
).withColumn("sum_curBalance_total", $"sum_curBalance_CreditCard" + $"sum_curBalance_micro" + $"sum_curBalance_169"
).withColumn("curBalance_total_binned",curBalance_12m_bin($"sum_curBalance_total")
)

src_out.coalesce(1).write.option("header", "true").option("sep", ";").mode("append").csv(output_path)
