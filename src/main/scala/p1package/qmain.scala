package p1package
import scala.io.Source
import scala.io.StdIn.readInt
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.SparkSession





object qmain extends App {




  def loginT(a: String): Unit = {
    var ty = a
    if (ty == "a" || ty == "A") {
      p1package.main.loginA()
    }
    if (ty == "b" || ty == "B") {
      p1package.main.loginB()
    }
  }

  def adminqueries(): Unit = {
    menuloopA()
  }
  def basicqueries(): Unit = {
    menuloopB()
  }
  var userin: Int = 0
  def menuA(): Int = {
    println("1:query 1")
    println("2:query 2")
    println("3:query 3")
    println("4:query 4")
    println("5:query 5")
    println("6:query 6")
    println("7 change username and password")
    println("0: End program")
    println("choose one")

    try {
      userin = readInt()
      return userin
    } catch {
      case e: NumberFormatException => 0
    }
  }
  def menuB(): Int = {
    println("1:query 1")
    println("2:query 2")
    println("3:query 3")
    println("4 change username and password")
    println("0: End program")
    println("choose one")

    try {
      userin = readInt()
      return userin
    } catch {
      case e: NumberFormatException => 0
    }
  }
  def menuSwitchA(i: Int): Unit = {
    i match {

      case 1 =>
      p1package.main.q1()

      case 2 =>
      p1package.main.q2()


      case 3 =>
      p1package.main.q3()


      case 4 =>
        p1package.main.q4()


      case 5 =>
      p1package.main.q5()

      case 6 =>
        p1package.main.q6()

      case  7=>
        p1package.main.changeA()
      case _ => {}
    }
  }
  def menuSwitchB(i: Int): Unit = {
    i match {

      case 1 =>
      p1package.main.q1()

      case 2 =>
      p1package.main.q2()

      case 3 =>
      p1package.main.q3()

      case 4=>
      p1package.main.changeB()
      case _ => {}
    }
  }
  def menuloopA(): Unit = {
    var choice = 0
    do {
      choice = menuA()
      menuSwitchA(choice)
    }
    while (choice != 0)
  }
  def menuloopB(): Unit = {
    var choice = 0
    do {
      choice = menuB()
      menuSwitchB(choice)
    }
    while (choice != 0)
  }




}
