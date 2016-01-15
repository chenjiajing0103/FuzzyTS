/**
  * Created by jiajing on 15-12-30.
  */
class FuzzyModel(val intervals: Array[Array[Double]],
                 val maxPriceSet: Int,
                 val maxItemSet: Int)
  extends Serializable {

  def score(trainTs: Vector[Vector[Double]], testTs: Vector[Vector[Double]]): Double = {
    val trainRate = TimeSeriesUtils.calRateByPrice(trainTs(0))
    val trainFuzzyPrice = TimeSeriesUtils.transRealToFuzzy(intervals(0), trainRate)
    val trainFuzzyVars = (for (i <- 1 until intervals.length) yield {
      TimeSeriesUtils.transRealToFuzzy(intervals(i), trainTs(i))
    }).toVector
    val rulesPriceOneOrder = TimeSeriesUtils.calFuzzyRulesUniOneOrder(trainFuzzyPrice, maxPriceSet)
    val rulesPriceTwoOrder = TimeSeriesUtils.calFuzzyRulesUniTwoOrder(trainFuzzyPrice, maxPriceSet)
    val rulesPriceThreeOrder = TimeSeriesUtils.calFuzzyRulesUniThreeOrder(trainFuzzyPrice, maxPriceSet)
    val rulesVarsOneOrder = (for (i <- trainFuzzyVars.indices) yield {
      TimeSeriesUtils.calFuzzyRulesMultiVarOneOrder(trainFuzzyVars(i), trainFuzzyPrice,
        maxItemSet, maxPriceSet)
    }).toArray
    val rulesVarsTwoOrder = (for (i <- trainFuzzyVars.indices) yield {
      TimeSeriesUtils.calFuzzyRulesMultiVarTwoOrder(trainFuzzyVars(i), trainFuzzyPrice,
        maxItemSet, maxItemSet, maxPriceSet)
    }).toArray
    val rulesVarsThreeOrder = (for (i <- trainFuzzyVars.indices) yield {
      TimeSeriesUtils.calFuzzyRulesMultiVarThreeOrder(trainFuzzyVars(i), trainFuzzyPrice,
        maxItemSet, maxItemSet, maxItemSet, maxPriceSet)
    }).toArray
    val testRate = TimeSeriesUtils.calRateByPrice(testTs(0))
    val testFuzzyPrice = TimeSeriesUtils.transRealToFuzzy(intervals(0), testRate)
    val testFuzzyVars = (for (i <- 1 until intervals.length) yield {
      TimeSeriesUtils.transRealToFuzzy(intervals(i), testTs(i))
    }).toVector
    val mids = TimeSeriesUtils.calMidPoints(intervals(0))

    val rate = TimeSeriesUtils.forecastMultiVarMultiOrd(mids, rulesPriceOneOrder,
      rulesPriceTwoOrder, rulesPriceThreeOrder, rulesVarsOneOrder, rulesVarsTwoOrder,
      rulesVarsThreeOrder, testFuzzyPrice, testFuzzyVars)
    val price = TimeSeriesUtils.calPriceByRate(rate, testTs(0))
    TimeSeriesUtils.calRMSE(testTs(0), price)
  }
}
