package TJFH.task

import java.sql.DriverManager
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object st_river_r {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    var start_tm = "2023-04-15"
    var end_tm = "2023-05-01"
    var is_current = "false"
    var sd = 2
    //获取历史记录最新时间
    var collect_tm = "2023-04-24"

    if(is_current.equals("true")){
      val his_max_tm_sql_tb = """ select max(tm)-0.1 as tm from md.st_river_r where data_source is null or data_source in('dzp_sq','lk') """
      val lastest_tm_df = readMD(spark,his_max_tm_sql_tb)
      val lastest_tm_cache = lastest_tm_df

      if(lastest_tm_cache.take(1)(0)(0) !=null ){
        println("st_river_r 前一次历史最新时间是:"+lastest_tm_cache.take(1)(0).getTimestamp(0))
        val timestamp = lastest_tm_cache.select("tm").take(1)(0).getTimestamp(0)
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val data_last_tm = formatter.format(timestamp.toLocalDateTime)
        println(data_last_tm)
        collect_tm = data_last_tm
      }else{
        println("st_river_r 前一次历史最新时间是Null，因此数据将从最早的时间采集")
        collect_tm= "2024-06-01"
      }
    }

    var tm = new ListBuffer[String]
    var sqktm = new ListBuffer[String]
    if(is_current == "true"){
      println(s"delete from md.st_river_r where tm>='$collect_tm'   and (data_source is null or data_source in('dzp_sq','lk'))")
      deleteData(s"delete from md.st_river_r where tm>='$collect_tm'   and (data_source is null or data_source in('dzp_sq','lk'))")
      println(s"==============tm>='$collect_tm'  delete finish")
      val strings = dateList(collect_tm, LocalDate.now().plusDays(1).toString+" 00:00:00", sd)
      val sqkstr = sqk_list(collect_tm, LocalDate.now().plusDays(1).toString+" 00:00:00", sd)
      tm = strings
      sqktm=sqkstr
    }else{
      println(s"delete from md.st_river_r where tm>='$start_tm' and tm<'$end_tm'   and (data_source is null or data_source in('dzp_sq','lk'))")
      deleteData(s"delete from md.st_river_r where tm>='$start_tm' and tm<'$end_tm'   and (data_source is null or data_source in('dzp_sq','lk'))")
      println(s"==============tm>='$start_tm' and tm<'$end_tm'  delete finish")
      val strings = dateList(start_tm, end_tm, sd)
      tm = strings
      val sqkstr = sqk_list(start_tm, end_tm, sd)
      sqktm=sqkstr
    }
    import spark.implicits._

    val st_list = readMD(spark,"select * from md.att_st_base").filter(col("st_type").isin("ZZ", "ZQ"))
      .select(col("st_code"))

    println(st_list.count())

    val broadcastCodes: Broadcast[Array[String]] = spark.sparkContext.broadcast(st_list.as[String].collect())

    val prop = new Properties()
    prop.put("user", "wqualitykz") //表示用户名
    prop.put("password", "kz2018") //表示密码
    prop.put("driver", "oracle.jdbc.driver.OracleDriver")

    val sqk = spark.read.jdbc(url="jdbc:oracle:thin:@10.12.4.29:1521/meetHydro",table="HYDROKZ.DZP_SQ",sqktm.toArray,prop)
      .filter(row => broadcastCodes.value.contains(row.getAs[String]("ZH")))
      .select(
        col("ZH").alias("guid"),
        col("ZH").alias("st_code"),
        col("YMDHM").alias("tm").cast("timestamp"),
        col("UP_SW").alias("z").cast("decimal(7,3)"),
        col("LL").alias("q").cast("decimal(9,3)"),
        lit(null).alias("xsa").cast("decimal(9,3)"),
        lit(null).alias("xsavv").cast("decimal(5,3)"),
        lit(null).alias("xsmxv").cast("decimal(5,3)"),
        lit(null).alias("flwchrcd").cast("string"),
        lit(null).alias("wptn").cast("string"),
        lit(null).alias("msqmt").cast("string"),
        lit(null).alias("msamt").cast("string"),
        lit(null).alias("msvmt").cast("string"),
        lit(current_timestamp()).alias("eff_time"),
        col("LEIXING").alias("data_type").cast("int"),
        col("RJLL").alias("q_avg").cast("decimal(7,3)"),
        col("XJLL").alias("xjll").cast("decimal(7,3)"),
        col("YJLL").alias("yjll").cast("decimal(7,3)")
      )



    val  res_rz = spark.read.jdbc(url="jdbc:oracle:thin:@10.12.4.29:1521/meetHydro",table="HYDROKZ.ST_RIVER_R",tm.toArray,prop)
      .select(
        col("STCD").alias("guid"),
        col("STCD").alias("st_code"),
        col("TM").alias("tm").cast("timestamp"),
        col("Z").alias("z").cast("decimal(7,3)"),
        col("Q").alias("q").cast("decimal(9,3)"),
        col("XSA").alias("xsa"),
        col("XSAVV").alias("xsavv").cast("decimal(5,3)"),
        col("XSMXV").alias("xsmxv").cast("decimal(5,3)"),
        col("FLWCHRCD").alias("flwchrcd"),
        col("WPTN").alias("wptn"),
        col("MSQMT").alias("msqmt"),
        col("MSAMT").alias("msamt"),
        col("MSVMT").alias("msvmt"),
        lit(current_timestamp()).alias("eff_time"),
        lit(null).alias("data_type").cast("int"),
        lit(null).alias("q_avg").cast("decimal(7,3)"),
        lit(null).alias("xjll").cast("decimal(7,3)"),
        lit(null).alias("yjll").cast("decimal(7,3)")
      ).join(sqk, Seq("guid", "st_code","tm"), "left_anti")

    val result = sqk.unionAll(res_rz)
    println(result.count())
    result.printSchema()
    result.show()

    writeRdjc(result,"st_river_r")

    spark.stop()
  }

  def readMD(spark: SparkSession,table:String): DataFrame ={
    val reader = spark.read.format("jdbc")
      .option("dirver", "com.kingbase8.Driver")
      .option("url", "jdbc:kingbase8://10.12.40.26:54321/tjfhdd")
      .option("dbtable", s"($table) AS t")
      .option("user", "kingbase")
      .option("password", "L#zst&r&j5vZ")
      .load()

    reader
  }

  def dateList(start:String,end:String,slid:Int): ListBuffer[String] ={
    val tm_list = new ListBuffer[String]()

    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val startDate = LocalDate.parse(start, dateFormatter)
    val endDate = LocalDate.parse(end, dateFormatter)
    //.minusDays(1)
    // 构建日期序列
    val dateRange = Iterator.iterate(startDate)(_ plusDays 1).takeWhile(!_.isAfter(endDate)).toList

    // 遍历日期序列，将连续的两个日期作为参数传入函数
    dateRange.sliding(slid).foreach(pair => {
      val tm_filter = "tm>=to_date('"+pair.head.format(dateFormatter)+ "','yyyy-mm-dd hh24:mi:ss') and tm<to_date('"+pair.last.format(dateFormatter)+"','yyyy-mm-dd hh24:mi:ss')"
      tm_list.append(tm_filter)
    })

    tm_list.foreach(r=>println("加载时间段:"+r))
    tm_list
  }

  def sqk_list(start:String,end:String,slid:Int): ListBuffer[String] ={
    val dzp_sq_list = new ListBuffer[String]()

    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val startDate = LocalDate.parse(start, dateFormatter)
    val endDate = LocalDate.parse(end, dateFormatter)
    //.minusDays(1)
    // 构建日期序列
    val dateRange = Iterator.iterate(startDate)(_ plusDays 1).takeWhile(!_.isAfter(endDate)).toList

    // 遍历日期序列，将连续的两个日期作为参数传入函数
    dateRange.sliding(slid).foreach(pair => {
      val tm_filter = "YMDHM>=to_date('"+pair.head.format(dateFormatter)+ "','yyyy-mm-dd hh24:mi:ss') and YMDHM<to_date('"+pair.last.format(dateFormatter)+"','yyyy-mm-dd hh24:mi:ss')"
      dzp_sq_list.append(tm_filter)
    })

    dzp_sq_list.foreach(r=>println("dzp_sq加载时间段:"+r))
    dzp_sq_list
  }

  def deleteData(sql:String): Unit = {
    // connect to Dm database
    val dmUrl = "jdbc:kingbase8://10.12.40.26:54321/tjfhdd"
    val dmUser = "kingbase"
    val dmPassword = "L#zst&r&j5vZ"
    val dmConn = DriverManager.getConnection(dmUrl, dmUser, dmPassword)
    dmConn.setAutoCommit(false)
    val insertStatement = dmConn.createStatement()
    val insertQuery = s"$sql"
    insertStatement.executeUpdate(insertQuery)
    dmConn.commit()
    dmConn.setAutoCommit(true)
    insertStatement.close()
    dmConn.close()
  }

  def writeRdjc(frame:DataFrame,table:String): Unit ={
    frame.write
      .mode("append")
      .format("jdbc")
      .option("url", "jdbc:kingbase8://10.12.40.26:54321/tjfhdd")
      .option("dbtable", s"md.$table")
      .option("user", "kingbase")
      .option("password", "L#zst&r&j5vZ")
      .save()
  }
}
