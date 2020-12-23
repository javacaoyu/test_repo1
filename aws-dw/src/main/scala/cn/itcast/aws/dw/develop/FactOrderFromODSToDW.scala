package cn.itcast.aws.dw.develop

import java.util.Properties

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 作者：传智播客
  * 描述：从ODS抽取数据字段，JSON拆分为独立字段并关联时间维度。
  */

object FactOrderFromODSToDW {
  def main(args: Array[String]): Unit = {
    val input = args(0)   // csv 的输入路径
    //    val input = """G:\07-TMP\HFT-S3-NO-REPEAT\non-sensitive\sp\"""
    val output = args(1)
    //    val output = "C:\\Users\\caoyu\\Desktop\\testout"
    val spark = SparkSession.builder()
      //      .master("local[*]")
      .appName("FactOrderFromODSToDW")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // source 1: csv.
    val sourceDF = spark.read.format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("quote", "\"")
      .load(input)

    // source 2: mysql dim date.
    val properties = new Properties()
    properties.setProperty("user","itcast")
    properties.setProperty("password","Passw0rd")
    properties.setProperty("driver", "com.amazon.redshift.jdbc42.Driver")
    val url = """jdbc:redshift://test-dw-redshift.cao5hdirnj6p.cn-northwest-1.redshift.amazonaws.com.cn:5439/itcast?useUnicode=true&characterEncoding=utf-8"""
    val sourceDimDateArray = spark.read.jdbc(url, "dw.dim_date", properties).collect()
    //两份数据来源都取到了。
    val dimDateCacheMap = mutable.Map[String, (String, String, Int, Int, String, String, Int, Int, String, String, String, String, String, Int, Long, String, Int, String, String)]()
    sourceDimDateArray.foreach(row => {
      val date_key = row.getString(0)
      val date_value = row.getString(1)
      val day_in_year = row.getInt(2)
      val day_in_month = row.getInt(3)
      val is_first_day_in_month = row.getString(4)
      val is_last_day_in_month = row.getString(5)
      val weekday = row.getInt(6)
      val week_in_month = row.getInt(7)
      val is_first_day_in_week = row.getString(8)
      val is_dayoff = row.getString(9)
      val is_workday = row.getString(10)
      val is_holiday = row.getString(11)
      val date_type = row.getString(12)
      val month_number = row.getInt(13)
      val year = row.getLong(14)
      val quarter_name = row.getString(15)
      val quarter_number = row.getInt(16)
      val year_quarter = row.getString(17)
      val year_month_number = row.getString(18)
      val tuple: (String, String, Int, Int, String, String, Int, Int, String, String, String, String, String, Int, Long, String, Int, String, String) =
        (
          date_key,date_value,day_in_year,day_in_month,
          is_first_day_in_month,is_last_day_in_month,weekday,week_in_month,
          is_first_day_in_week,is_dayoff,is_workday,is_holiday,date_type,
          month_number,year,quarter_name,quarter_number,year_quarter,year_month_number
        )
      dimDateCacheMap.put(date_value, tuple)
    })

    val orderDefineDFWithNull: Dataset[ArrayBuffer[OrderINFODefine]] = sourceDF.map(row => {
      val orderID = row.getString(0)
      val dateStr = row.getString(1)
      val orderTS = row.getString(2).toLong
      // 因为源文件，Glue输出的时候，对JSON使用了双引号包围，所以这里对其进行处理，转换为标准JSON字符串。
      val dataJSONStr = row.getString(3)
        .replace("""""""", """"""")
        .replace(""""{""", "{")
        .replace("""}"""", "}")
      var dataJSON: JSONObject = null
      try{
        dataJSON = new JSONObject(dataJSONStr)
      }catch {
        case ex:org.json.JSONException => {
          ex.printStackTrace()
          println("数据不符合JSON规范，跳过本行内容，内容为：" + dataJSONStr)
        }
      }

      if (dataJSON == null){
        null
      }else{
        val productArray: JSONArray = dataJSON.getJSONArray("product")
        val defineArray = ArrayBuffer[OrderINFODefine]()
        for (i <- 0 until productArray.length()) {
          val productJSON = productArray.getJSONObject(i)
          val productCount: Int = productJSON.getInt("count")
          val productName: String = productJSON.getString("name")
            .replace(" ", "")
            .replace("'", "")
            .replace("""|""", "")
            .replace("""\""", "")
            .replace(""""""", "").stripMargin
          val productUnitID: Int = productJSON.getInt("unitID")
          val productBarcode: String = productJSON.getString("barcode")
          val productPrice: Double = productJSON.getDouble("pricePer")
          val productRetailPrice: Double = productJSON.getDouble("retailPrice")
          val productTradePrice: Double = productJSON.getDouble("tradePrice")
          val productCategoryID: Int = productJSON.getInt("categoryID")

          // Starting define fields for OrderINFODefine class
          val uniqueID = orderID + "_" + productBarcode

          // part 1 store info
          val storeProvince = dataJSON.getString("storeProvince")
          val storeCity = dataJSON.getString("storeCity")
          val storeDistrict = dataJSON.getString("storeDistrict")
          var storeGpsLongitude = dataJSON.getString("storeGPSLongitude")
          if(storeGpsLongitude == "undefined"){
            storeGpsLongitude = "0"
          }
          var storeGpsLatitude = dataJSON.getString("storeGPSLatitude")
          if(storeGpsLatitude == "undefined"){
            storeGpsLatitude = "0"
          }
          val storeCreatedTS = dataJSON.getLong("storeCreateDateTS")
          val storeAddress = dataJSON.getString("storeAddress")
          var storeGpsAddress = dataJSON.getString("storeGPSAddress")
          if(storeGpsAddress == "None"){
            storeGpsAddress = "未知GPS地址"
          }
          val storeOwnUserID = dataJSON.getInt("storeOwnUserId")
          val storeOwnUserName = dataJSON.getString("storeOwnUserName")
          val storeName = dataJSON.getString("storeName")
          val storeOwnUserTel = dataJSON.getLong("storeOwnUserTel")
          val storeID = dataJSON.getInt("storeID")
          val isSigned = dataJSON.getInt("isSigned")

          // part 2 money info
          val discount = dataJSON.getDouble("discount")
          val discountRate = dataJSON.getDouble("discountRate")
          val productTotalCountFromOrder = dataJSON.getDouble("productCount")
          val payType = dataJSON.getString("payType")
          val smallChange = dataJSON.getDouble("smallChange")
          val receivable = dataJSON.getDouble("receivable")
          val afterDiscount = dataJSON.getDouble("moneyBeforeWholeDiscount")
          val totalNoDiscount = dataJSON.getDouble("totalNoDiscount")
          val erase = dataJSON.getDouble("erase")
          val payTotal = dataJSON.getDouble("payedTotal")
          val memberID = dataJSON.getString("memberID")

          // part 3 time info
          //        (String, String, Int, Int, String, String, Int, Int, String, String, String, String, String, Int, Int, String, Int, String, String)
          val nullTuple = ("", "", 0, 0, "", "", 0, 0, "", "", "", "", "", 0, 0L, "", 0, "", "")
          val dimDateTuple = dimDateCacheMap.getOrElse(dateStr, nullTuple)
          val date_key = dimDateTuple._1
          val date_value = dimDateTuple._2
          val day_in_year = dimDateTuple._3
          val day_in_month = dimDateTuple._4
          val is_first_day_in_month = dimDateTuple._5
          val is_last_day_in_month = dimDateTuple._6
          val weekday = dimDateTuple._7
          val week_in_month = dimDateTuple._8
          val is_first_day_in_week = dimDateTuple._9
          val is_dayoff = dimDateTuple._10
          val is_workday = dimDateTuple._11
          val is_holiday = dimDateTuple._12
          val date_type = dimDateTuple._13
          val month_number = dimDateTuple._14
          val year: Long = dimDateTuple._15
          val quarter_name = dimDateTuple._16
          val quarter_number = dimDateTuple._17
          val year_quarter = dimDateTuple._18
          val year_month_number = dimDateTuple._19

          // 构建输出Schema cass类对象
          val orderDefine = OrderINFODefine(
            unique_id = uniqueID,
            order_id = orderID,
            order_date = dateStr,
            order_ts = orderTS,
            store_district = storeDistrict,
            store_province = storeProvince,
            store_city = storeCity,
            store_gps_longitude = storeGpsLongitude,
            store_gps_latitude = storeGpsLatitude,
            store_created_ts = storeCreatedTS,
            store_address = storeAddress,
            store_gps_address = storeGpsAddress,
            store_own_user_id = storeOwnUserID,
            store_own_user_name = storeOwnUserName,
            store_name = storeName,
            store_own_user_tel = storeOwnUserTel,
            store_id = storeID,
            is_signed = isSigned,
            discount = discount,
            discount_rate = discountRate,
            product_count = productTotalCountFromOrder,
            pay_type = payType,
            small_change = smallChange,
            receivable = receivable,
            after_discount = afterDiscount,
            total_no_discount = totalNoDiscount,
            erase = erase,
            pay_total = payTotal,
            member_id = memberID,
            product_name = productName,
            sold_count = productCount,
            barcode = productBarcode,
            price = productPrice,
            retail_price = productRetailPrice,
            trade_price = productTradePrice,
            category_id = productCategoryID,
            date_key = date_key,
            date_value = date_value,
            day_in_year = day_in_year,
            day_in_month = day_in_month,
            is_first_day_in_month = is_first_day_in_month,
            is_last_day_in_month = is_last_day_in_month,
            weekday = weekday,
            week_in_month = week_in_month,
            is_first_day_in_week = is_first_day_in_week,
            is_dayoff = is_dayoff,
            is_workday = is_workday,
            is_holiday = is_holiday,
            date_type = date_type,
            month_number = month_number,
            year = year,
            quarter_name = quarter_name,
            quarter_number = quarter_number,
            year_quarter = year_quarter,
            year_month_number = year_month_number
          )
          defineArray.append(orderDefine)
        }
        defineArray
      }
    })

    val orderDefineDFWithoutNull = orderDefineDFWithNull.filter(x => x!=null)
    val orderDefineDF: Dataset[OrderINFODefine] = orderDefineDFWithoutNull.flatMap(x => x)

    orderDefineDF.write.format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", "|")
      .save(output)
    spark.close()
  }
}
