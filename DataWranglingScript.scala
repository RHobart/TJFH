package cn.gistack.gdmp.datawrangling.script

import cn.gistack.gdmp.datawrangling.script.service.IDataWranglingScript
import java.sql.DriverManager
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * @author SuperHuang
 * @date 2021/12/06 10:32
 * */

class DataWranglingScript extends BaseDataWranglingScript with IDataWranglingScript {
  override def executeScript(): Unit = {
  val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    var start_tm = "2024-06-14"
    var end_tm = "2024-06-16"
    var is_current = "true"
    var sd = 2
    //获取历史记录最新时间
    var collect_tm = "2023-04-24"

    if(is_current.equals("true")){
      val his_max_tm_sql_tb = """ select max(tm)-0.5 as tm from md.st_pump_r where data_source != 'i'  """
      val lastest_tm_df = readMD(spark,his_max_tm_sql_tb)
      val lastest_tm_cache = lastest_tm_df

      if(lastest_tm_cache.take(1)(0)(0) !=null ){
        println("st_pump_r 前一次历史最新时间是:"+lastest_tm_cache.take(1)(0).getTimestamp(0))
        val timestamp = lastest_tm_cache.select("tm").take(1)(0).getTimestamp(0)
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val data_last_tm = formatter.format(timestamp.toLocalDateTime)
        println(data_last_tm)
        collect_tm = data_last_tm
      }else{
        println("st_pump_r 前一次历史最新时间是Null，因此数据将从最早的时间采集")
        collect_tm= "2024-06-01"
      }
    }

    var share_tm = new ListBuffer[String]
    var sqk_tm = new ListBuffer[String]
    var lk_tm = new ListBuffer[String]


    if(is_current == "true"){
      println(s"delete from md.st_pump_r where tm>='$collect_tm' and data_source!='i' ")
      deleteData(s"delete from md.st_pump_r where tm>='$collect_tm' and data_source!='i' ")
      println(s"==============tm>='$collect_tm'  delete finish")
      share_tm = dateList(collect_tm, LocalDate.now().plusDays(1).toString+" 00:00:00", sd,"tm>='","' and tm<'","'")
      sqk_tm = dateList(collect_tm, LocalDate.now().plusDays(1).toString+" 00:00:00", sd,"YMDHM>=to_date('","','yyyy-mm-dd hh24:mi:ss') and YMDHM<to_date('","','yyyy-mm-dd hh24:mi:ss')")
      lk_tm = dateList(collect_tm, LocalDate.now().plusDays(1).toString+" 00:00:00", sd,"tm>=to_date('","','yyyy-mm-dd hh24:mi:ss') and tm<to_date('","','yyyy-mm-dd hh24:mi:ss')")
    }else{
      println(s"delete from md.st_pump_r where tm>='$start_tm' and tm<'$end_tm' and data_source!='i' ")
      deleteData(s"delete from md.st_pump_r where tm>='$start_tm' and tm<'$end_tm' and data_source!='i' ")
      println(s"==============tm>='$start_tm' and tm<'$end_tm'  delete finish")
      share_tm = dateList(start_tm, end_tm, sd,"tm>='","' and tm<'","'")
      sqk_tm = dateList(start_tm, end_tm, sd,"YMDHM>=to_date('","','yyyy-mm-dd hh24:mi:ss') and YMDHM<to_date('","','yyyy-mm-dd hh24:mi:ss')")
      lk_tm = dateList(start_tm, end_tm, sd,"tm>=to_date('","','yyyy-mm-dd hh24:mi:ss') and tm<to_date('","','yyyy-mm-dd hh24:mi:ss')")
    }

    import spark.implicits._
    val st_list = readMD(spark,"select * from md.att_st_base").filter(col("st_type").isin("DP"))
      .select(col("st_code"))
    val broadcastCodes: Broadcast[Array[String]] = spark.sparkContext.broadcast(st_list.as[String].collect())

    val pgrop = new Properties()
    pgrop.put("user", "tianjinshare") //表示用户名
    pgrop.put("password", "Tianjin@Share") //表示密码
    pgrop.put("driver", "org.postgresql.Driver")
    val new_st_pump_r = spark.read.jdbc(url = "jdbc:postgresql://101.33.249.21:46962/tianjinshare", table = "public.st_pump_r", share_tm.toArray, pgrop)
      .select(
        col("guid").alias("guid"),
        col("st_code").alias("st_code"),
        col("tm").alias("tm").cast("timestamp"),
        col("ppupz").alias("ppupz").cast("decimal(7,3)"),
        col("ppdwz").alias("ppdwz").cast("decimal(7,3)"),
        col("omcn").alias("omcn").cast("decimal(3,0)"),
        col("ompwr").alias("ompwr").cast("decimal(5,0)"),
        col("pmpq").alias("pmpq").cast("decimal(7,3)"),
        col("ppwchrcd").alias("ppwchrcd").cast("string"),
        col("ppupwptn").alias("ppupwptn").cast("string"),
        lit(null).alias("ppdwwptn").cast("string"),
        col("msqmt").alias("msqmt").cast("string"),
        col("pdchcd").alias("pdchcd").cast("string"),
        lit("new").alias("data_source"),
        lit(current_timestamp()).alias("eff_time"),
        lit(1).alias("data_type").cast("int"),
        lit(null).alias("q_avg").cast("decimal(7,3)"),
        lit(null).alias("xjll").cast("decimal(7,3)"),
        lit(null).alias("yjll").cast("decimal(7,3)")
      )

    val prop = new Properties()
    prop.put("user", "wqualitykz") //表示用户名
    prop.put("password", "kz2018") //表示密码
    prop.put("driver", "oracle.jdbc.driver.OracleDriver")

    val sqk = spark.read.jdbc(url="jdbc:oracle:thin:@10.12.4.29:1521/meetHydro",table="HYDROKZ.DZP_SQ",sqk_tm.toArray,prop)
      .filter(row => broadcastCodes.value.contains(row.getAs[String]("ZH")))
      .select(
        col("ZH").alias("guid"),
        col("ZH").alias("st_code"),
        col("YMDHM").alias("tm").cast("timestamp"),
        col("UP_SW").alias("ppupz").cast("decimal(7,3)"),
        lit(null).alias("ppdwz").cast("decimal(7,3)"),
        lit(null).alias("omcn").cast("decimal(3,0)"),
        lit(null).alias("ompwr").cast("decimal(5,0)"),
        lit(null).alias("pmpq").cast("decimal(7,3)"),
        lit(null).alias("ppwchrcd").cast("string"),
        lit(null).alias("ppupwptn").cast("string"),
        lit(null).alias("ppdwwptn").cast("string"),
        lit(null).alias("msqmt").cast("string"),
        lit(null).alias("pdchcd").cast("string"),
        lit(null).alias("data_source").cast("string"),
        lit(current_timestamp()).alias("eff_time"),
        col("LEIXING").alias("data_type").cast("int"),
        col("RJLL").alias("q_avg").cast("decimal(7,3)"),
        col("XJLL").alias("xjll").cast("decimal(7,3)"),
        col("YJLL").alias("yjll").cast("decimal(7,3)")
    )


    val  pump_rz = spark.read.jdbc(url="jdbc:oracle:thin:@10.12.4.29:1521/meetHydro",table="HYDROKZ.ST_PUMP_R",lk_tm.toArray,prop)
      .select(
      col("STCD").alias("guid"),
      col("STCD").alias("st_code"),
      col("TM").alias("tm").cast("timestamp"),
      col("PPUPZ").alias("ppupz").cast("decimal(7,3)"),
      col("PPDWZ").alias("ppdwz").cast("decimal(7,3)"),
      col("OMCN").alias("omcn").cast("decimal(3,0)"),
      col("OMPWR").alias("ompwr").cast("decimal(5,0)"),
      col("PMPQ").alias("pmpq").cast("decimal(7,3)"),
      col("PPWCHRCD").alias("ppwchrcd").cast("string"),
      col("PPUPWPTN").alias("ppupwptn").cast("string"),
      col("PPDWWPTN").alias("ppdwwptn").cast("string"),
      col("MSQMT").alias("msqmt").cast("string"),
      col("PDCHCD").alias("pdchcd").cast("string"),
      lit(null).alias("data_source").cast("string"),
      lit(current_timestamp()).alias("eff_time"),
      lit(1).alias("data_type").cast("int"),
      lit(null).alias("q_avg").cast("decimal(7,3)"),
      lit(null).alias("xjll").cast("decimal(7,3)"),
      lit(null).alias("yjll").cast("decimal(7,3)")
    )

    pump_rz.show()
    pump_rz.printSchema()

    val result = sqk.unionAll(pump_rz).unionAll(new_st_pump_r)

    writeRdjc(result,"st_pump_r")

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

  def dateList(start:String,end:String,slid:Int,sta:String,mid:String,ends:String): ListBuffer[String] ={
    val tm_list = new ListBuffer[String]()

    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val startDate = LocalDateTime.parse(start, dateFormatter)
    val endDate = LocalDateTime.parse(end, dateFormatter)
    //.minusDays(1)
    // 构建日期序列
    val dateRange = Iterator.iterate(startDate)(_ plusHours 1).takeWhile(!_.isAfter(endDate)).toList

    // 遍历日期序列，将连续的两个日期作为参数传入函数
    dateRange.sliding(slid).foreach(pair => {
      val tm_filter = s"$sta"+pair.head.format(dateFormatter)+ s"$mid"+pair.last.format(dateFormatter)+s"$ends"
      tm_list.append(tm_filter)
    })

    tm_list.foreach(r=>println("加载时间段:"+r))
    tm_list
  }


  def deleteData(sql:String): Unit = {
    // connect to Dm database
    val dmUrl = "jdbc:kingbase8://10.12.40.26:54321/tjfhdd"
    val dmUser = "kingbase"
    val dmPassword = "L#zst&r&j5vZ"
    val dmConn = DriverManager.getConnection(dmUrl, dmUser, dmPassword)
    // execute an insert statement to write the data to Dm database
    dmConn.setAutoCommit(false)
    val insertStatement = dmConn.createStatement()
    val insertQuery = s"$sql"
    insertStatement.executeUpdate(insertQuery)
    dmConn.commit()
    dmConn.setAutoCommit(true)
    // close the Dm connection
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
