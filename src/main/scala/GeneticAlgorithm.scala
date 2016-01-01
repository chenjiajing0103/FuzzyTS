import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by jiajing on 15-12-30.
  */
class GeneticAlgorithm (val repeatTimes: Int,
                        val trainTimes: Int,
                        val populationSize: Int,
                        val maxPriceSet: Int,
                        val eliteSize: Int,
                        val tournamentSize: Int,
                        val mutationRate: Double)
  extends Serializable {

  private val pricePopulation = new Array[Array[Double]](populationSize)
  private val nextPricePopulation = new Array[Array[Double]](populationSize)
  private val fitness = new Array[Double](populationSize)
  private var rateMax = Double.MaxValue
  private var rateMin = Double.MinValue

  def fit(ts: Vector[Double]): FuzzyModel = {
    //(0 until repeatTimes).map{ x =>
      val rateTs = TimeSeriesUtils.calRateByPrice(ts)
      rateMax = rateTs.max
      rateMin = rateTs.min
      var bestPriceInterval = initIntervals(rateMax, rateMin)
      for (i <- 0 to trainTimes) {
        evaluate(rateTs)
        bestPriceInterval = elitist()
        crossover()
        mutate()
        //intervalCheck()
        lengthCheck()
        copyNextToCur()
      }
      new FuzzyModel(bestPriceInterval, maxPriceSet)
    //}.toArray
  }

  private def initIntervals(max: Double, min: Double): Array[Double] = {
    // one more interval_point(larger step) to cover the last ts_point
    val step = (max - min) / (maxPriceSet - 1)
    for (i <- 0 until populationSize) {
      val gene = new ArrayBuffer[Double]
      for (j <- 1 until maxPriceSet) {
        // not contain the start and end ts_point
        gene.append(min + step * j)
      }
      pricePopulation(i) = gene.toArray
      //sort
    }
    pricePopulation(0)
  }

  private def evaluate(ts: Vector[Double]): Unit = {
    // price intervals
    pricePopulation.indices.foreach(i => {
      //for (i <- pricePopulation.indices)
      val fuzzySeries = TimeSeriesUtils.transRealToFuzzy(pricePopulation(i), ts)
      val rules = TimeSeriesUtils.calFuzzyRules(fuzzySeries, maxPriceSet)
      val mids = TimeSeriesUtils.calMidPoints(pricePopulation(i))
      val rate = TimeSeriesUtils.forecast(mids, rules, fuzzySeries)
      val price = TimeSeriesUtils.calPriceByRate(rate, ts)
      fitness(i) = TimeSeriesUtils.calRMSE(ts, price)
    })
  }

  private def elitist(): Array[Double] = {
    val idxFit = new Array[(Int, Double)](populationSize)
    for (i <- fitness.indices) {
      idxFit(i) = (i, fitness(i))
    }
    idxFit.sortBy(_._2)
    for (i <- 0 until eliteSize) {
      nextPricePopulation(i) = pricePopulation(idxFit(i)._1)
    }
    pricePopulation(idxFit(0)._1)
  }

  private def select(): Int = {
    val selectIdxs = (0 until tournamentSize).map(x =>
      Random.nextInt(populationSize)).toArray
    selectIdxs.minBy(i => fitness(i))
  }

  private def crossover(): Unit = {
    for (i <- eliteSize until (populationSize, 2)) {
      var idx1 = 0
      var idx2 = 0
      while (idx1 == idx2) {
        idx1 = select()
        idx2 = select()
      }
      singlePointCrossover(idx1, idx2, i)
    }
  }

  private def singlePointCrossover(idx1: Int, idx2: Int, nextIdx: Int): Unit = {
    val split1 = Random.nextInt(pricePopulation(idx1).length - 1) + 1
    val split2 = Random.nextInt(pricePopulation(idx2).length - 1) + 1
    nextPricePopulation(nextIdx) = pricePopulation(idx1).slice(0, split1) ++
      pricePopulation(idx2).slice(split2, pricePopulation(idx2).length)
    nextPricePopulation(nextIdx + 1) = pricePopulation(idx2).slice(0, split2) ++
      pricePopulation(idx1).slice(split1, pricePopulation(idx1).length)
  }

  private def mutate(): Unit = {
    for (i <- 0 until populationSize if Random.nextDouble() < mutationRate) {
      nextPricePopulation(i) = (Random.nextInt(3) match {
        case 0 => insertMutate(i)
        case 1 => changeMutate(i)
        case 2 => removeMutate(i)
      }).sorted
    }
  }

  private def insertMutate(idx: Int): Array[Double] = {
    if (nextPricePopulation(idx).length < maxPriceSet - 1) {
      nextPricePopulation(idx) :+ (Random.nextDouble()*(rateMax - rateMin) + rateMin)
    }  else {
      nextPricePopulation(idx)
    }
  }

  private def changeMutate(idx: Int): Array[Double] = {
    val factor = Random.nextDouble()* 0.4 + 0.1
    val elem = Random.nextInt(nextPricePopulation(idx).length)
    Random.nextInt(2) match {
      case 0 => nextPricePopulation(idx)(elem) *= (1 - factor)
      case 1 => nextPricePopulation(idx)(elem) *= (1 + factor)
    }
    nextPricePopulation(idx)
  }

  private def removeMutate(idx: Int): Array[Double] = {
    val elem = Random.nextInt(nextPricePopulation(idx).length)
    val len = nextPricePopulation(idx).length
    if (len > 2) {
      nextPricePopulation(idx).slice(0, elem) ++
        nextPricePopulation(idx).slice(elem + 1, len)
      //nextPricePopulation(idx)(elem) = nextPricePopulation(idx).last
      //nextPricePopulation(idx).dropRight(1)
    } else {
      nextPricePopulation(idx)
    }
  }


  private def intervalCheck(): Unit = {
    for (i <- nextPricePopulation.indices) {
      nextPricePopulation(i) = clearAdhesion(nextPricePopulation(i))
    }
  }

  private def clearAdhesion(gene: Array[Double]): Array[Double] = {
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

  private def lengthCheck(): Unit = {
    for (i <- nextPricePopulation.indices) {
      while (nextPricePopulation(i).length >= maxPriceSet) {
        nextPricePopulation(i) = removeMutate(i)
      }
    }
  }

  private def copyNextToCur(): Unit = {
    for (i <- nextPricePopulation.indices) {
      pricePopulation(i) = nextPricePopulation(i)
    }
  }

}

