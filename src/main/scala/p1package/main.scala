package p1package

import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.SparkSession
import p1package.qmain.{adminqueries, basicqueries}

object main extends App {

  BasicConfigurator.configure()
  //System.setProperty("hadoop.home.dir","C:\\winutils")
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("buzz")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  //println("spark session created!!")
  //spark.sql("DROP table IF EXISTS musictable")
  //spark.sql("create table IF NOT EXISTS musictable(position Int,song String,artist String,streams Int,date String,genre String) row format delimited fields terminated by ','")
  //spark.sql("LOAD DATA INPATH 'C:/Users/jalen/IdeaProjects/project1/input/music.csv' INTO TABLE musictable")

  //spark.sql("create table IF NOT EXISTS admin(username String,password String) row format delimited fields terminated by ','")
  //spark.sql("LOAD DATA INPATH 'C:/Users/jalen/IdeaProjects/project1/loginA.csv' INTO TABLE admin")

  var basicuser= spark.sql("select username from basic").collect
  var bu=basicuser(0).toString().substring(1)
  var adminuser=spark.sql("select username from admin").collect
  var au=adminuser(0).toString().substring(1)
  var basicpass=spark.sql("select password from basic").collect
  var bp=basicpass(0).toString().substring(1)
  var adminpass=spark.sql("select password from admin").collect
  var ap=adminpass(0).toString().substring(1)
  println(bu)
  var logintypes = scala.io.StdIn.readLine("login type, A for Admin, B for Basic: ")
  var logintype = logintypes.toString
 //println(basicuser(0).toString())
 qmain.loginT(logintype)

  //spark.sql("select * from musictable where position=1").show(50)
  def q1(): Unit = {
    println("what is the average number of streams at position # 1")
    spark.sql("select round(avg(streams)) as average_streams from musictable where position=1").show
  }

  def q2(): Unit = {
    println("what is the most prevalent genre in position 1")
    spark.sql("select genre, count(genre) as appearances from musictable where position=1 group by genre order by appearances desc").show(200)
  }

  def q3(): Unit = {
    println("what 5 artist appears most often")
    spark.sql("select artist, count(artist) as appearances from musictable group by artist order by appearances desc").show(5)
  }

  def q4(): Unit = {
    println("what 5 artist have the most streams")
    spark.sql("select artist, sum(streams) as total_streams from musictable group by artist order by total_streams desc").show(5)
  }

  def q5(): Unit = {
    println("what 5 songs have the most streams")
    spark.sql("select song, max(streams) as highest_streams from musictable group by song order by highest_streams desc").show(5)
  }

  def q6(): Unit = {
    println("what songs appears most likely to appear in the top 10")
    spark.sql("select first(artist),song,count(song) as appearances from musictable where position<11 group by song order by appearances desc").show(5)

  }
  //spark.sql("describe musictable").show()
  // spark.sql("show tables").show()
  // spark.sql("select * from musictable").show()


  def changeA():Unit ={
    var newU= scala.io.StdIn.readLine(" enter desired Username: ")
    var newP= scala.io.StdIn.readLine(" enter new password: ")
    var confirm= scala.io.StdIn.readLine(" connfirm password: ")

    if(newP==confirm){

      spark.sql("drop table admin")
      spark.sql("create table IF NOT EXISTS admin(username String,password String) row format delimited fields terminated by ','")
      spark.sql(s"insert into admin(username,password) values ('$newU', '$newP')")
    }else print("passwords didnt match")
  }
  def changeB():Unit ={
    var newU= scala.io.StdIn.readLine(" enter desired Username: ")
    var newP= scala.io.StdIn.readLine(" enter new password: ")
    var confirm= scala.io.StdIn.readLine(" connfirm password: ")

    if(newP==confirm){

      spark.sql("drop table basic")
      spark.sql("create table IF NOT EXISTS basic(username String,password String) row format delimited fields terminated by ','")
      spark.sql(s"insert into basic(username,password) values ('$newU', '$newP')")
    }else print("passwords didnt match")
  }
  def loginA(): Unit = {

    var susername = scala.io.StdIn.readLine(" enter Username:  ")
    if (au.substring(0,au.length-1) == susername) {
      var spassword = scala.io.StdIn.readLine("enter password:  ")
      if (spassword == ap.substring(0,ap.length-1)) {
        adminqueries()
      } else println("password doesnt match")
    }
    else println("username doesnt match")

  }
  def loginB(): Unit = {

    var susername = scala.io.StdIn.readLine("Username: ")
    if (bu.substring(0,bu.length-1) == susername) {
      var spassword = scala.io.StdIn.readLine("password: ")
      if (spassword == bp.substring(0,bp.length-1)) {
        basicqueries()
      } else println("password dosent match")
    }
    else println("username doesent match")

  }
}
