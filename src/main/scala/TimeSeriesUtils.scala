/**
  * Created by jiajing on 15-12-30.
  */
object TimeSeriesUtils {
  def calRateByPrice(ts: Vector[Double]): Vector[Double] = {
    (for (i <- 0 until ts.size -1) yield {
      (ts(i + 1) - ts(i)) / ts(i)
      //(ts(i + 1) - ts(i)) / ts(i) * 100
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

  def calFuzzyRulesUniOneOrder(fs: Vector[Int], dim: Int)
  : Array[Array[Int]] = {
    val PriceRules = Array.ofDim[Int](dim, dim)
    //DenseMatrix.zeros[Double](maxPriceSet, maxPriceSet)
    for (i <- 0 until fs.size - 1) {
      PriceRules(fs(i))(fs(i+1)) += 1
    }
    PriceRules
  }

  def calFuzzyRulesUniTwoOrder(fs: Vector[Int], dim: Int)
  : Array[Array[Array[Int]]] = {
    val PriceRules = Array.ofDim[Int](dim, dim, dim)
    //DenseMatrix.zeros[Double](maxPriceSet, maxPriceSet)
    for (i <- 0 until fs.size - 2) {
      PriceRules(fs(i))(fs(i+1))(fs(i+2)) += 1
    }
    PriceRules
  }

  def calFuzzyRulesUniThreeOrder(fs: Vector[Int], dim: Int)
  : Array[Array[Array[Array[Int]]]] = {
    val PriceRules = Array.ofDim[Int](dim, dim, dim, dim)
    //DenseMatrix.zeros[Double](maxPriceSet, maxPriceSet)
    for (i <- 0 until fs.size - 3) {
      PriceRules(fs(i))(fs(i+1))(fs(i+2))(fs(i+3)) += 1
    }
    PriceRules
  }

  def calFuzzyRulesMultiVarOneOrder(fsVar: Vector[Int], fsPrice: Vector[Int],
                         dimX: Int, dimY: Int)
  : Array[Array[Int]] = {
    val varsRules = Array.ofDim[Int](dimX, dimY)
    for (i <- 0 until fsPrice.size - 1) {
      varsRules(fsVar(i))(fsPrice(i+1)) += 1
    }
    varsRules
  }

  def calFuzzyRulesMultiVarTwoOrder(fsVar: Vector[Int], fsPrice: Vector[Int],
                        dimX: Int, dimY: Int, dimZ: Int)
  : Array[Array[Array[Int]]] = {
    val varsRules = Array.ofDim[Int](dimX, dimY, dimZ)
    for (i <- 0 until fsPrice.size - 2) {
      varsRules(fsVar(i))(fsVar(i+1))(fsPrice(i+2)) += 1
      //varsRules(fsVar(i))(fsVar(i+1))(fsPrice(i+1)) += 1
    }
    varsRules
  }

  def calFuzzyRulesMultiVarThreeOrder(fsVar: Vector[Int], fsPrice: Vector[Int],
                                    dimX: Int, dimY: Int, dimZ: Int, dimK: Int)
  : Array[Array[Array[Array[Int]]]] = {
    val varsRules = Array.ofDim[Int](dimX, dimY, dimZ, dimK)
    for (i <- 0 until fsPrice.size - 3) {
      varsRules(fsVar(i))(fsVar(i+1))(fsVar(i+2))(fsPrice(i+3)) += 1
      //varsRules(fsVar(i))(fsVar(i+1))(fsVar(i+2))(fsPrice(i+1)) += 1
    }
    varsRules
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
      for (j <- mids.indices if rulesPrice(fsPrice(i))(j) != 0) {
      //for (j <- mids.indices if rulesPrice(fsPrice(i))(fsPrice(j)) != 0) {
        up += rulesPrice(fsPrice(i))(fsPrice(j)) * mids(j)
        down += rulesPrice(fsPrice(i))(fsPrice(j))
      }
      val priceRate = if (down > 0) up / down else 0
      val varsRate = for (k <- fsVars.indices) yield {
        var varsUp = 0.0
        var varsDown = 0.0
        for (j <- mids.indices if rulesVars(k)(fsVars(k)(i))(j) != 0) {
        //for (j <- mids.indices if rulesVars(k)(fsVars(k)(i))(fsPrice(j)) != 0) {
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

  def forecastMultiVarMultiOrd(
      mids: Array[Double],
      rulesPriceOneOrd: Array[Array[Int]],
      rulesPriceTwoOrd: Array[Array[Array[Int]]],
      rulesPriceThreeOrd: Array[Array[Array[Array[Int]]]],
      rulesVarsOneOrd: Array[Array[Array[Int]]],
      rulesVarsTwoOrd: Array[Array[Array[Array[Int]]]],
      rulesVarsThreeOrd: Array[Array[Array[Array[Array[Int]]]]],
      fsPrice: Vector[Int],
      fsVars: Vector[Vector[Int]])
  : Vector[Double] = {
    (for (i <- 0 until fsPrice.size - 1 ) yield {
      var up = 0.0
      var down = 0.0
      /*for (j <- mids.indices) {
        if (rulesPriceOneOrd(fsPrice(i))(fsPrice(j)) != 0) {
          up += rulesPriceOneOrd(fsPrice(i))(fsPrice(j)) * mids(j)
          down += rulesPriceOneOrd(fsPrice(i))(fsPrice(j))
        }
        if (i > 0 && rulesPriceTwoOrd(fsPrice(i-1))(fsPrice(i))(fsPrice(j)) != 0) {
          up += rulesPriceTwoOrd(fsPrice(i-1))(fsPrice(i))(fsPrice(j)) * mids(j)
          down += rulesPriceTwoOrd(fsPrice(i-1))(fsPrice(i))(fsPrice(j))
        }
        if (i > 1 && rulesPriceThreeOrd(fsPrice(i-2))(fsPrice(i-1))(fsPrice(i))(fsPrice(j)) != 0) {
          up += rulesPriceThreeOrd(fsPrice(i-2))(fsPrice(i-1))(fsPrice(i))(fsPrice(j)) * mids(j)
          down += rulesPriceThreeOrd(fsPrice(i-2))(fsPrice(i-1))(fsPrice(i))(fsPrice(j))
        }
      }
      val priceRate = if (down > 0) up / down else 0*/
      val varsRate = for (k <- fsVars.indices) yield {
        var varsUp = 0.0
        var varsDown = 0.0
        for (j <- mids.indices) {
          if (rulesVarsOneOrd(k)(fsVars(k)(i))(j) != 0) {
          //if (rulesVarsOneOrd(k)(fsVars(k)(i))(fsPrice(j)) != 0) {
            varsUp += rulesVarsOneOrd(k)(fsVars(k)(i))(fsPrice(j)) * mids(j)
            varsDown += rulesVarsOneOrd(k)(fsVars(k)(i))(fsPrice(j))
          }
          if (i > 0 && rulesVarsTwoOrd(k)(fsVars(k)(i-1))(fsVars(k)(i))(j) != 0) {
          //if (i > 0 && rulesVarsTwoOrd(k)(fsVars(k)(i-1))(fsVars(k)(i))(fsPrice(j)) != 0) {
            varsUp += rulesVarsTwoOrd(k)(fsVars(k)(i-1))(fsVars(k)(i))(fsPrice(j)) * mids(j)
            varsDown += rulesVarsTwoOrd(k)(fsVars(k)(i-1))(fsVars(k)(i))(fsPrice(j))
          }
          if (i > 1 && rulesVarsThreeOrd(k)(fsVars(k)(i-2))(fsVars(k)(i-1))(fsVars(k)(i))(j) != 0) {
          //if (i > 1 && rulesVarsThreeOrd(k)(fsVars(k)(i-2))(fsVars(k)(i-1))(fsVars(k)(i))(fsPrice(j)) != 0) {
            varsUp += rulesVarsThreeOrd(k)(fsVars(k)(i-2))(fsVars(k)(i-1))(fsVars(k)(i))(fsPrice(j)) * mids(j)
            varsDown += rulesVarsThreeOrd(k)(fsVars(k)(i-2))(fsVars(k)(i-1))(fsVars(k)(i))(fsPrice(j))
          }
        }
        up += varsUp
        down += varsDown
        if (varsDown > 0) varsUp / varsDown else 0
      }
      if (down > 0) up / down else 0
    }).toVector
  }

  def calMidPoints(gene: Array[Double]): Array[Double] = {
    val numOfPriceSet = gene.length + 1
    (for (i <- 1 to numOfPriceSet) yield { // to maxPriceSet
      i match {
        case 1 => gene(0) - 0.002
        case `numOfPriceSet` => gene(gene.length - 1) +0.002
        case _ => (gene(i - 1) + gene(i - 2)) / 2.0
      }
    }).toArray
  }

  def calPriceByRate(rs: Vector[Double], ts: Vector[Double])
  : Vector[Double] = {
    // return not contain ts(0)
    (for (i <- rs.indices) yield {
      //ts(i + 1) * (1 + rs(i))
      ts(i + 1) * (1 + rs(i) / 100)
    }).toVector
  }

  def calTestPriceByRate(rs: Vector[Double], ts: Vector[Double])
  : Vector[Double] = {
    // return not contain ts(0)
    ts(0) +: ts(1) +: (for (i <- rs.indices) yield {
      //ts(i + 1) * (1 + rs(i))
      ts(i + 1) * (1 + rs(i) / 100)
    }).toVector
  }

  def calRMSE(ts: Vector[Double], ps: Vector[Double])
  : Double = {
    Math.sqrt((for (i <- ps.indices) yield {
      Math.pow(Math.abs(ps(i) - ts(i + 2)), 2) //ts(i+1)
    }).sum / ps.size)
  }

  def calTestRMSE(ts: Vector[Double], ps: Vector[Double])
  : Double = {
    Math.sqrt((for (i <- ps.indices) yield {
      Math.pow(Math.abs(ps(i) - ts(i)), 2)
    }).sum / ps.size)
  }
}
