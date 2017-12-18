
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.{mutable}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap


object Main {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollaborativeFiltering").setMaster("local[*]")
    val sc = new SparkContext(conf)

    var data = sc.textFile("input/ratings.csv")
    var header = data.first()
    data = data.filter(row => row != header)

    var testData=sc.textFile("input/toBePredicted.csv")
    val header2 = testData.first()
    testData = testData.filter(row=> row != header2)


    var testKeys=new mutable.HashSet[(Int,Int)]()

    val test = testData.map(_.split(',') match { case Array(user, item) => (user.toInt, item.toInt)})
    test.collect().foreach{x=>testKeys+=((x._1,x._2))}

    case class Rating(user: Int, product: Int, rating: Double)


    var allRatings = data.map(_.split(',') match { case Array(user, item, rate, ts) => Rating(user.toInt, item.toInt, rate.toDouble)})

    val ratings=allRatings.filter(x=>testKeys.contains(x.user,x.product)!=true)

    val item_userRatings=ratings.groupBy(x=>x.product).collectAsMap()

    val item_userRatings1=ratings.groupBy(x=>x.product)

    val user_itemRatings=ratings.groupBy(x=>x.user).collectAsMap()

    val userItem_rating=ratings.map { case Rating(user, product, rate) => ((user, product),rate)}.collectAsMap()

    def getRating(user:Int,item:Int): Double =
    {
      val ret=userItem_rating.get((user, item))
      return ret.get
    }

    val avgUsers_ratingMap=ratings.map(x=>(x.user,(x.rating,1))).reduceByKey((x1,x2)=>(x1._1+x2._1,x1._2+x2._2)).mapValues(x=>x._1/x._2).collectAsMap()

    def predict(activeUser:Int,activeItem:Int,neighbourhoodK:Int): Double = {


      var activeItemsUsers=Set[Int]()
      var activeUserItems=Set[Int]()
      var guessedValue:Double=2.5
      if(avgUsers_ratingMap.contains(activeUser)) {
        guessedValue = avgUsers_ratingMap(activeUser)
      }


      if(item_userRatings.contains(activeItem))
      {
        item_userRatings.get(activeItem).get.foreach(x => activeItemsUsers += x.user)
      }
      user_itemRatings.get(activeUser).get.foreach(x => activeUserItems += x.product)



      def getItemWeight(item: (Int, Iterable[Rating])): (Double, Int) = {

        var weight_ratings = new mutable.HashMap[Double, ListBuffer[Int]]()

        var summationNumerator: Double = 0
        var summationDenominator1: Double = 0
        var summationDenominator2: Double = 0
        if (item._1 != activeItem) {
          for (rating <- item._2) {
            val coRaterUser = rating.user
            if (activeItemsUsers.contains(coRaterUser)) {

              val avgUserRating = avgUsers_ratingMap.get(coRaterUser).get
              val normalisedActiveItemRating = getRating(coRaterUser, activeItem) - avgUserRating
              val normalisedCoItemRating = rating.rating - avgUserRating

              summationNumerator = summationNumerator + (normalisedActiveItemRating * normalisedCoItemRating)
              summationDenominator1 = summationDenominator1 + math.pow(normalisedActiveItemRating, 2)
              summationDenominator2 = summationDenominator2 + math.pow(normalisedCoItemRating, 2)
            }
          }
        }
        val weightPassiveItem = summationNumerator / (math.sqrt(summationDenominator1) * math.sqrt(summationDenominator2))
        if (!weightPassiveItem.isNaN) {
          return (BigDecimal(weightPassiveItem).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble, item._1)
        }
        else {
          return (Double.MinValue,0)
        }
      }


      var weightList=new ListBuffer[(Double,Int)]
      for (elem <- item_userRatings) {
        if(activeUserItems.contains(elem._1))
        {
          val weight=getItemWeight(elem)
          weightList+=weight
        }
      }
      val sorted_Weight_Item_List=weightList.sortWith((x1,x2)=>x1._1>x2._1)

      var predictionNumerator: Double = 0
      var predictionDenominator: Double = 0
      var evaled = 0

      for (rateditem <- sorted_Weight_Item_List) {
        if (evaled <= neighbourhoodK) {
          if (activeUserItems.contains(rateditem._2)) {
            evaled = evaled + 1

            predictionNumerator = predictionNumerator + (getRating(activeUser, rateditem._2) * rateditem._1)
            predictionDenominator = predictionDenominator + rateditem._1
          }
        }
      }


      val prediction = predictionNumerator / predictionDenominator
      if(prediction.isNaN)
      {

        return guessedValue
      }
      else
      {
        if(prediction>5)
        {
          return 5.0
        }
        if(prediction<0)
        {
          return 0.0
        }
        return prediction
      }

    }

    def predictAll(toBePredicted_user_item:RDD[(Int, Int)],neighbourhoodK:Int): RDD[((Int, Int), Double)] =
    {

      return toBePredicted_user_item.map(x=>((x._1,x._2),predict(x._1,x._2,neighbourhoodK)))
    }

    val neighbourhoodK = 9

    new File("RatingPredictions.txt" ).delete()
    val pw = new PrintWriter(new File("RatingPredictions.txt" ))
    val Predictions=predictAll(test,neighbourhoodK)




    val ratesAndPreds = allRatings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(Predictions)



    var buckets =  new HashMap[(Int,String), Int]()
    buckets.put((1,">=0and<1"),0)
    buckets.put((2,">=1and<2"),0)
    buckets.put((3,">=2and<3"),0)
    buckets.put((4,">=3and<4"),0)
    buckets.put((5,">=4"),0)


    val accuracy = ratesAndPreds.map(x=>x._2._1-x._2._2).collect()

    for (arr<-accuracy) {
      var absolute_accuracy=arr
      if(arr < 0 )
      {
        absolute_accuracy=arr * -1.0
      }

      if (absolute_accuracy >= 0 && absolute_accuracy < 1) {
        buckets.put((1,">=0and<1"),buckets.get(1,">=0and<1").get + 1)
      }
      else if (absolute_accuracy >= 1 && absolute_accuracy < 2) {
        buckets.put((2,">=1and<2"), buckets.get(2,">=1and<2").get + 1)
      }
      else if (absolute_accuracy >= 2 && absolute_accuracy < 3) {
        buckets.put((3,">=2and<3"), buckets.get(3,">=2and<3").get + 1)
      }
      else if (absolute_accuracy >= 3 && absolute_accuracy < 4) {
        buckets.put((4,">=3and<4"), buckets.get(4,">=3and<4").get + 1)
      }
      else if (absolute_accuracy >= 4) {
        buckets.put((5,">=4"), buckets.get(5,">=4").get + 1)
      }
    }
    for(x<-buckets.toList.sortBy(x=>x._1._1)){
      println(x._1._2+", "+x._2)
    }



    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()


    val print_output = Predictions.collect()
    pw.println("Mean Squared Error = " + MSE)
    pw.println("UserID,MovieID,Pred_ratings")
    for(x<-print_output){
      pw.println(x._1._1+","+x._1._2+","+x._2)
    }
    pw.close()

  }


}
