import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by jiajing on 15-12-30.
  */
class GeneticAlgorithm (val repeatTimes: Int,
                        val trainTimes: Int,
                        val populationSize: Int,
                        val maxPriceSet: Int,
                        val maxItemSet: Int,
                        val eliteSize: Int,
                        val tournamentSize: Int,
                        val crossoverRate: Double,
                        val mutationRate: Double)
  extends Serializable {
  // popIdx, varIdx, intervalIdx
  private val populations = Array.ofDim[Double](populationSize, 5, 10)
  private val nextPopulations = Array.ofDim[Double](populationSize, 5, 10)
  private val fitness = new Array[Double](populationSize)
  private var rateMax = Double.MaxValue
  private var rateMin = Double.MinValue
  private var varsMax = new Array[Double](0)
  private var varsMin = new Array[Double](0)

  def fit(ts: Vector[Vector[Double]]): Array[FuzzyModel] = {
    require(ts.nonEmpty)

    val rateTs = TimeSeriesUtils.calRateByPrice(ts(0))
    //println(rateTs)
    (0 until repeatTimes).map{ x =>
      var bestIntervals = initIntervals(ts, rateTs)
      //var bestIntervals = initIntervalsByRandom(ts, rateTs)
      for (i <- 0 to trainTimes) {
        //populations.foreach(x => println(x(0).mkString(",")))
        evaluate(ts, rateTs)
        bestIntervals = elitist()
        crossover(ts.length)
        mutate(ts.length)
        intervalCheck()
        lengthCheck(ts.length)
        copyNextToCur()
      }
      new FuzzyModel(bestIntervals, maxPriceSet, maxItemSet)
    }.toArray
  }

  private def initIntervals(ts: Vector[Vector[Double]],
                            rateTs: Vector[Double]): Array[Array[Double]] = {
    rateMax = rateTs.max
    rateMin = rateTs.min
    val otherMax = for (k <- 1 until ts.length) yield ts(k).max
    val otherMin = for (k <- 1 until ts.length) yield ts(k).min
    // one more interval_point(larger step) to cover the last ts_point
    val step = (rateMax - rateMin) / (maxPriceSet - 1)
    val otherStep = for (k <- 0 until ts.length - 1) yield
      (otherMax(k) - otherMin(k)) / (maxItemSet - 1)

    for (i <- 0 until populationSize) {
      // price col
      val genePrice = new ArrayBuffer[Double]
      for (j <- 1 until maxPriceSet - 1) { // -1
        // not contain the start and end ts_point
        genePrice.append(rateMin + step * j)
      }
      //sort

      // other var cols
      val otherPops = for (k <- 0 until ts.length - 1) yield {
        val geneOther = new ArrayBuffer[Double]
        for (j <- 1 until maxItemSet - 1) { // -1
          // not contain the start and end ts_point
          geneOther.append(otherMin(k) + otherStep(k) * j)
        }
        geneOther.toArray
      }
      populations(i) = (genePrice.toArray +: otherPops).toArray
    }

    varsMax = otherMax.toArray
    varsMin = otherMin.toArray
    populations(0)
  }

  private def initIntervalsByRandom(ts: Vector[Vector[Double]],
                            rateTs: Vector[Double]): Array[Array[Double]] = {
    rateMax = rateTs.max
    rateMin = rateTs.min
    val otherMax = for (k <- 1 until ts.length) yield ts(k).max
    val otherMin = for (k <- 1 until ts.length) yield ts(k).min

    for (i <- 0 until populationSize) {
      // price col
      val rateRange = rateMax - rateMin
      val genePrice = new ArrayBuffer[Double]
      for (j <- 1 until maxPriceSet) {
        genePrice.append(Random.nextDouble()*rateRange + rateMin)
      }

      // other var cols
      val otherPops = for (k <- 0 until ts.length - 1) yield {
        val otherRange = otherMax(k) - otherMin(k)
        val geneOther = new ArrayBuffer[Double]
        for (j <- 1 until maxItemSet) {
          // not contain the start and end ts_point
          geneOther.append(Random.nextDouble()*otherRange + otherMin(k))
        }
        geneOther.sorted.toArray
      }
      populations(i) = (genePrice.sorted.toArray +: otherPops).toArray
    }

    varsMax = otherMax.toArray
    varsMin = otherMin.toArray
    populations(0)
  }

  private def evaluate(ts: Vector[Vector[Double]],
                       rateTs: Vector[Double]): Unit = {
    (0 until populationSize).foreach(popIdx => {
      val fuzzyPrice = TimeSeriesUtils.transRealToFuzzy(populations(popIdx)(0), rateTs)
      val fuzzyVars = (for (i <- 1 until ts.length) yield {
        TimeSeriesUtils.transRealToFuzzy(populations(popIdx)(i), ts(i))
      }).toVector
      val rulesPriceOneOrder = TimeSeriesUtils.calFuzzyRulesUniOneOrder(fuzzyPrice, maxPriceSet)
      val rulesPriceTwoOrder = TimeSeriesUtils.calFuzzyRulesUniTwoOrder(fuzzyPrice, maxPriceSet)
      val rulesPriceThreeOrder = TimeSeriesUtils.calFuzzyRulesUniThreeOrder(fuzzyPrice, maxPriceSet)
      val rulesVarsOneOrder = (for (i <- fuzzyVars.indices) yield {
        TimeSeriesUtils.calFuzzyRulesMultiVarOneOrder(fuzzyVars(i), fuzzyPrice,
          maxItemSet, maxPriceSet)
      }).toArray
      val rulesVarsTwoOrder = (for (i <- fuzzyVars.indices) yield {
        TimeSeriesUtils.calFuzzyRulesMultiVarTwoOrder(fuzzyVars(i), fuzzyPrice,
          maxItemSet, maxItemSet, maxPriceSet)
      }).toArray
      val rulesVarsThreeOrder = (for (i <- fuzzyVars.indices) yield {
        TimeSeriesUtils.calFuzzyRulesMultiVarThreeOrder(fuzzyVars(i), fuzzyPrice,
          maxItemSet, maxItemSet, maxItemSet, maxPriceSet)
      }).toArray
      val midsPrice = TimeSeriesUtils.calMidPoints(populations(popIdx)(0))
      val rate = TimeSeriesUtils.forecastMultiVarMultiOrd(midsPrice, rulesPriceOneOrder,
        rulesPriceTwoOrder, rulesPriceThreeOrder, rulesVarsOneOrder, rulesVarsTwoOrder,
        rulesVarsThreeOrder, fuzzyPrice, fuzzyVars)
      val price = TimeSeriesUtils.calPriceByRate(rate, ts(0))
      fitness(popIdx) = TimeSeriesUtils.calRMSE(ts(0), price)
    })
  }

  private def evaluateByValidation(ts: Vector[Vector[Double]],
                       rateTs: Vector[Double]): Unit = {
    (0 until populationSize).foreach(popIdx => {
      val fuzzyPrice = TimeSeriesUtils.transRealToFuzzy(populations(popIdx)(0), rateTs)
      val fuzzyVars = (for (i <- 1 until ts.length) yield {
        TimeSeriesUtils.transRealToFuzzy(populations(popIdx)(i), ts(i))
      }).toVector
      val rulesPriceOneOrder = TimeSeriesUtils.calFuzzyRulesUniOneOrder(fuzzyPrice, maxPriceSet)
      val rulesPriceTwoOrder = TimeSeriesUtils.calFuzzyRulesUniTwoOrder(fuzzyPrice, maxPriceSet)
      val rulesPriceThreeOrder = TimeSeriesUtils.calFuzzyRulesUniThreeOrder(fuzzyPrice, maxPriceSet)
      val rulesVarsOneOrder = (for (i <- fuzzyVars.indices) yield {
        TimeSeriesUtils.calFuzzyRulesMultiVarOneOrder(fuzzyVars(i), fuzzyPrice,
          maxItemSet, maxPriceSet)
      }).toArray
      val rulesVarsTwoOrder = (for (i <- fuzzyVars.indices) yield {
        TimeSeriesUtils.calFuzzyRulesMultiVarTwoOrder(fuzzyVars(i), fuzzyPrice,
          maxItemSet, maxItemSet, maxPriceSet)
      }).toArray
      val rulesVarsThreeOrder = (for (i <- fuzzyVars.indices) yield {
        TimeSeriesUtils.calFuzzyRulesMultiVarThreeOrder(fuzzyVars(i), fuzzyPrice,
          maxItemSet, maxItemSet, maxItemSet, maxPriceSet)
      }).toArray
      val midsPrice = TimeSeriesUtils.calMidPoints(populations(popIdx)(0))
      val rate = TimeSeriesUtils.forecastMultiVarMultiOrd(midsPrice, rulesPriceOneOrder,
        rulesPriceTwoOrder, rulesPriceThreeOrder, rulesVarsOneOrder, rulesVarsTwoOrder,
        rulesVarsThreeOrder, fuzzyPrice, fuzzyVars)
      val price = TimeSeriesUtils.calPriceByRate(rate, ts(0))
      fitness(popIdx) = TimeSeriesUtils.calRMSE(ts(0), price)
    })
  }

  private def elitist(): Array[Array[Double]] = {
    val idxFit = new Array[(Int, Double)](populationSize)
    for (i <- fitness.indices) {
      idxFit(i) = (i, fitness(i))
    }
    val sortedIdxFit = idxFit.sortBy(_._2)
    //println(fitness(sortedIdxFit(0)._1))
    for (i <- 0 until eliteSize) {
      nextPopulations(i) = populations(sortedIdxFit(i)._1).clone()
    }
    populations(sortedIdxFit(0)._1).clone()
  }

  private def crossover(tsLength: Int): Unit = {
    for (i <- eliteSize until (populationSize, 2)) {
      var idx1 = 0
      var idx2 = 0
      while (idx1 == idx2) {
        idx1 = select()
        idx2 = select()
      }
      if (Random.nextDouble() < crossoverRate) {
        singlePointCrossover(idx1, idx2, i, tsLength)
      } else {
        nextPopulations(i) = populations(idx1).clone()
        nextPopulations(i+1) = populations(idx2).clone()
      }
    }
  }

  private def select(): Int = {
    val selectIdxs = sample(0 until populationSize toVector, tournamentSize, Nil)
    //val selectIdxs = (0 until tournamentSize).map(x =>
    //  Random.nextInt(populationSize)).toArray
    selectIdxs.minBy(i => fitness(i))
  }

  private def sample(vect: Vector[Int], sampleSize: Int, acc: List[Int]): List[Int] = {
    if (sampleSize == 0) acc
    else {
      val index = Random.nextInt(vect.size)
      sample(vect.updated(index, vect(0)).tail, sampleSize - 1, vect(index) :: acc)
    }
  }

  private def singlePointCrossover(idx1: Int, idx2: Int, nextIdx: Int,
                                  tsLength: Int): Unit = {
    var split1 = Random.nextInt(populations(idx1)(0).length - 1) + 1
    var split2 = Random.nextInt(populations(idx2)(0).length - 1) + 1
    nextPopulations(nextIdx)(0) = populations(idx1)(0).slice(0, split1) ++
      populations(idx2)(0).slice(split2, populations(idx2)(0).length)//.sorted
    nextPopulations(nextIdx + 1)(0) = populations(idx2)(0).slice(0, split2) ++
      populations(idx1)(0).slice(split1, populations(idx1)(0).length)//.sorted

    (1 until tsLength).foreach(i => {
      split1 = Random.nextInt(populations(idx1)(i).length - 1) + 1
      split2 = Random.nextInt(populations(idx2)(i).length - 1) + 1
      nextPopulations(nextIdx)(i) = populations(idx1)(i).slice(0, split1) ++
        populations(idx2)(i).slice(split2, populations(idx2)(i).length)//.sorted
      nextPopulations(nextIdx + 1)(i) = populations(idx2)(i).slice(0, split2) ++
        populations(idx1)(i).slice(split1, populations(idx1)(i).length)//.sorted
    })
  }

  private def mutate(tsLength: Int): Unit = {
    (0 until tsLength).foreach( k => {
      //for (i <- 0 until populationSize if Random.nextDouble() < mutationRate) {
      for (i <- eliteSize until populationSize if Random.nextDouble() < mutationRate) {
        nextPopulations(i)(k) = (Random.nextInt(3) match {
          case 0 => insertMutate(i, k)
          case 1 => changeMutate(i, k)
          case 2 => removeMutate(i, k)
        }).sorted
      }
    })
  }

  private def insertMutate(popIdx: Int, varIdx: Int)
  : Array[Double] = {
    if (varIdx == 0) {
      // price
      if (nextPopulations(popIdx)(varIdx).length < maxPriceSet) { // -1
        nextPopulations(popIdx)(varIdx) :+ (Random.nextDouble()*(rateMax - rateMin) + rateMin)
      }  else {
        nextPopulations(popIdx)(varIdx)
      }
    } else {
      // other var
      if (nextPopulations(popIdx)(varIdx).length < maxItemSet) { // -1
        nextPopulations(popIdx)(varIdx) :+
          (Random.nextDouble()*(varsMax(varIdx - 1) - varsMin(varIdx - 1)) + varsMin(varIdx - 1))
      } else {
        nextPopulations(popIdx)(varIdx)
      }
    }
  }

  private def changeMutate(popIdx: Int, varIdx: Int)
  : Array[Double] = {
    val factor = Random.nextDouble()* 0.4 + 0.1
    val len = nextPopulations(popIdx)(varIdx).length
    val elem = Random.nextInt(len)
    Random.nextInt(2) match {
      case 0 => nextPopulations(popIdx)(varIdx).slice(0, elem) ++
        nextPopulations(popIdx)(varIdx).slice(elem + 1, len) :+
        nextPopulations(popIdx)(varIdx)(elem)*(1 - factor)
      //nextPopulations(popIdx)(varIdx)(elem) *= (1 - factor)
      case 1 => nextPopulations(popIdx)(varIdx).slice(0, elem) ++
        nextPopulations(popIdx)(varIdx).slice(elem + 1, len) :+
        nextPopulations(popIdx)(varIdx)(elem)*(1 + factor)
      //nextPopulations(popIdx)(varIdx)(elem) *= (1 + factor)
    }
  }

  private def removeMutate(popIdx: Int, varIdx: Int): Array[Double] = {
    val len = nextPopulations(popIdx)(varIdx).length
    val elem = Random.nextInt(len)
    if (len > 2) {
      nextPopulations(popIdx)(varIdx).slice(0, elem) ++
        nextPopulations(popIdx)(varIdx).slice(elem + 1, len)
      //nextPricePopulation(idx)(elem) = nextPricePopulation(idx).last
      //nextPricePopulation(idx).dropRight(1)
    } else {
      nextPopulations(popIdx)(varIdx)
    }
  }

  private def intervalCheck(): Unit = {
    for (i <- eliteSize until nextPopulations.length) {
      for (j <- nextPopulations(0).indices) {
        nextPopulations(i)(j) = clearAdhesion(nextPopulations(i)(j))
      }
    }
  }

  private def clearAdhesion(gene:Array[Double]): Array[Double] = {
    val minGap = 0.0005
    val minRoc = 0.1
    var size = gene.length
    val geneBuffer = new ArrayBuffer[Double]()
    var i = 0
    while (i < size - 1) {
      if (Math.abs(gene(i + 1) - gene(i)) < minGap ||
        Math.abs((gene(i + 1) - gene(i)) / gene(i)) < minRoc) {
        size -= 1
      } else {
        geneBuffer.append(gene(i))
      }
      i += 1
    }
    geneBuffer.append(gene.last)
    if (geneBuffer.size > 1) geneBuffer.toArray else gene
  }

  private def clearOneAdhesion(gene: Array[Double]): Array[Double] = {
    // has exception
    val minGap = 0.0005
    val minRoc = 0.1
    val intervalGap = for (i <- 0 until gene.length - 1) yield {
      Math.abs(gene(i + 1) - gene(i))
    }
    val intervalRoc = for (i <- 0 until gene.length - 1) yield {
      Math.abs((gene(i + 1) - gene(i)) / gene(i))
    }
    /*val adhesiveGap = (for (i <- 0 until gene.length - 1) yield {
      Math.abs(gene(i + 1) - gene(i))
    }).indexWhere(_ < minGap)
    val adhesiveRoc = (for (i <- 0 until gene.length - 1) yield {
      Math.abs((gene(i + 1) - gene(i)) / gene(i))
    }).indexWhere(_ < minRoc)*/
    val adhesiveIdx = (0 until gene.length - 1).
      indexWhere(i => intervalGap(i) < minGap || intervalRoc(i) < minRoc)
    gene.slice(0, adhesiveIdx) ++ gene.slice(adhesiveIdx + 1, gene.length)
  }

  private def clearAllAdhesion(gene: Array[Double]): Array[Double] = {
    // has exception
    val minGap = 0.0005
    val minRoc = 0.1
    val intervalGap = for (i <- 0 until gene.length - 1) yield {
      Math.abs(gene(i + 1) - gene(i))
    }
    val intervalRoc = for (i <- 0 until gene.length - 1) yield {
      Math.abs((gene(i + 1) - gene(i)) / gene(i))
    }
    gene.filter(elem => {
      val idx = gene.indexOf(elem)
      if (idx == gene.length - 1)
        true
      else {
        intervalGap(idx) < minGap || intervalRoc(idx) < minRoc
      }
    })
    /*val intervalGap = (for (i <- 0 until gene.length - 1) yield {
      gene(i) -> Math.abs(gene(i + 1) - gene(i))
    }).toMap
    val intervalRoc = (for (i <- 0 until gene.length - 1) yield {
      gene(i) -> Math.abs((gene(i + 1) - gene(i)) / gene(i))
    }).toMap
    gene.dropRight(1).filter(elem => intervalGap(elem) < minGap
      || intervalRoc(elem) < minRoc) :+ gene(gene.length - 1)*/

  }

  private def lengthCheck(tsLength: Int): Unit = {
    for (i <- eliteSize until nextPopulations.length) {
      while (nextPopulations(i)(0).length >= maxPriceSet) {
        nextPopulations(i)(0) = removeMutate(i, 0)
      }
      (1 until tsLength).foreach(j => {
        while (nextPopulations(i)(j).length >= maxItemSet) {
          nextPopulations(i)(j) = removeMutate(i, j)
        }
      })
    }
  }

  private def copyNextToCur(): Unit = {
    for (i <- nextPopulations.indices) {
      populations(i) = nextPopulations(i)
    }
  }

}

