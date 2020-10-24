import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    if (args.length != 3) {
      println("Usage: input_file number_of_iterations output_dir")
    }

    val input = sc.textFile(args(0))
    val number_of_iterations = args(1)
    val output_file =args(2)
    val initial_PR = 10.0



    val rdd_input = input.map(x => x.split(","))
    var airports = rdd_input.map(x => (x(0), x(1)))
    val firstRow = airports.first
    val airport_names = airports.filter(x => x != firstRow)
    val allairports = (airport_names.map { case (x, y) => x }.distinct() ++ airport_names.map { case (x, y) => y }.distinct()).distinct().collect()
    val out_links = airport_names.groupByKey().map(x => (x._1, x._2.size)).collect().map(x => (x._1, x._2)).toMap
    val rank = collection.mutable.Map() ++ allairports.map(x => (x, initial_PR)).toMap


    for (i <- 1 to number_of_iterations.toInt) {
      val out = collection.mutable.Map() ++ allairports.map(x => (x, 0.0)).toMap
      rank.keys.foreach((id) => rank(id) = rank(id) / out_links(id))
      for ((key, value) <- airport_names.collect()) {
        out.put(value, out(value) + rank(key))
      }
      val out1 = collection.mutable.Map() ++ out.map(x => (x._1, ((0.15 / allairports.size) + (1-0.15) * x._2)))
      out1.keys.foreach((id) => rank(id) = out1(id))
    }

    val result = rank.toSeq.sortBy(-_._2)
    sc.parallelize(result).saveAsTextFile(output_file)
  }
}
