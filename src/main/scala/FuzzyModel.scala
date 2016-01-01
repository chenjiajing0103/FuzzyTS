/**
  * Created by jiajing on 15-12-30.
  */
class FuzzyModel(val priceInterval: Array[Double],
                 val maxPriceSet: Int)
  extends Serializable {

  def score(trainTs: Vector[Double], testTs: Vector[Double]): Double = {
    val trainRate = TimeSeriesUtils.calRateByPrice(trainTs)
    val trainFuzzy = TimeSeriesUtils.transRealToFuzzy(priceInterval, trainRate)
    val rules =  TimeSeriesUtils.calFuzzyRules(trainFuzzy, maxPriceSet)
    val testRate = TimeSeriesUtils.calRateByPrice(testTs)
    val testFuzzy = TimeSeriesUtils.transRealToFuzzy(priceInterval, testRate)
    val mids = TimeSeriesUtils.calMidPoints(priceInterval)
    val rate = TimeSeriesUtils.forecast(mids, rules, testFuzzy)
    val price = TimeSeriesUtils.calPriceByRate(rate, testTs)
    TimeSeriesUtils.calRMSE(testTs, price)
  }
}
