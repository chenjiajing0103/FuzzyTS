/**
  * Created by jiajing on 15-12-30.
  */
object TimeSeriesUtils {
  def calRateByPrice(ts: Vector[Double]): Vector[Double] = {
    (for (i <- 0 until ts.size -1) yield {
      (ts(i + 1) - ts(i)) / ts(i)
    }).toVector
  }

  def transRealToFuzzy(interval: Array[Double], ts: Vector[Double])
  : Vector[Int] = {
    (for (i <- ts.indices) yield {
      val index = interval.indexWhere(_ > ts(i))
      if (index != -1) index else interval.length //maxPriceSet - 1
      //interval.find(_ > ts(i)).get
      //interval.collectFirst({case x if x > ts(i) => x}).get
    }).toVector
  }

  def calFuzzyRulesUni(fs: Vector[Int], dim: Int)
  : Array[Array[Int]] = {
    val PriceRules = Array.ofDim[Int](dim, dim)
    //DenseMatrix.zeros[Double](maxPriceSet, maxPriceSet)
    for (i <- 0 until fs.size - 1) {
      PriceRules(fs(i))(fs(i+1)) += 1
    }
    PriceRules
  }

  def calFuzzyRulesMulti(fsVar: Vector[Int], fsPrice: Vector[Int],
                         dimX: Int, dimY: Int)
  : Array[Array[Int]] = {
    val VarsRules = Array.ofDim[Int](dimX, dimY)
    for (i <- 0 until fsPrice.size - 1) {
      VarsRules(fsVar(i))(fsPrice(i+1)) += 1
    }
    VarsRules
  }

  def forecastUniSeries(mids: Array[Double],
               rules: Array[Array[Int]],
               fs: Vector[Int])
  : Vector[Double] = {
    (for (i <- 0 until fs.size - 1 ) yield {
      var up = 0.0
      var down = 0.0
      for (j <- mids.indices if rules(fs(i))(fs(j)) != 0) {
        up += rules(fs(i))(fs(j)) * mids(j)
        down += rules(fs(i))(fs(j))
      }
      if (down > 0) up / down else 0
    }).toVector
  }

  def forecastMultiVar(mids: Array[Double],
                      rulesPrice: Array[Array[Int]],
                      rulesVars: Array[Array[Array[Int]]],
                      fsPrice: Vector[Int],
                      fsVars: Vector[Vector[Int]])
  : Vector[Double] = {
    (for (i <- 0 until fsPrice.size - 1 ) yield {
      var up = 0.0
      var down = 0.0
      for (j <- mids.indices if rulesPrice(fsPrice(i))(fsPrice(j)) != 0) {
        up += rulesPrice(fsPrice(i))(fsPrice(j)) * mids(j)
        down += rulesPrice(fsPrice(i))(fsPrice(j))
      }
      val priceRate = if (down > 0) up / down else 0
      val varsRate = for (k <- 1 until fsVars.length) yield {
        var varsUp = 0.0
        var varsDown = 0.0
        for (j <- mids.indices if rulesVars(k)(fsVars(k)(i))(fsPrice(j)) != 0) {
          varsUp += rulesVars(k)(fsVars(k)(i))(fsPrice(j)) * mids(j)
          varsDown += rulesVars(k)(fsVars(k)(i))(fsPrice(j))
        }
        up += varsUp
        down += varsDown
        if (varsDown > 0) varsUp / varsDown else 0
      }
      if (down > 0) up / down else 0
    }).toVector
  }

  def forecastMultiVarMultiOrd(): Unit = {

  }

  def calMidPoints(gene: Array[Double]): Array[Double] = {
    val numOfPriceSet = gene.length + 1
    (for (i <- 1 to numOfPriceSet) yield { // to maxPriceSet
      i match {
        case 1 => gene(0) - 0.002
        case `numOfPriceSet` => gene(gene.length - 1) +0.002
        case _ => gene(i - 1) + gene(i - 2) / 2.0
      }
    }).toArray
  }

  def calPriceByRate(rs: Vector[Double], ts: Vector[Double])
  : Vector[Double] = {
    // return not contain ts(0)
    (for (i <- rs.indices) yield {
      ts(i) * (1 + rs(i))
    }).toVector
  }

  def calRMSE(ts: Vector[Double], ps: Vector[Double])
  : Double = {
    Math.sqrt((for (i <- ps.indices) yield {
      Math.pow(Math.abs(ps(i) - ts(i + 1)), 2)
    }).sum / ps.size)
  }

}
