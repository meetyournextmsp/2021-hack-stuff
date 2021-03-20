import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MakeLookup {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("Scottish Constituency & Region Lookup").getOrCreate
        val smallUser = spark.read.format("csv").option("header", "true").load("SmallUser.csv")
        val constituency = spark.read.format("csv").option("header", "true").load("constituencylookup.csv")
        val region = spark.read.format("csv").option("header", "true").load("regionlookup.csv")
        smallUser.createOrReplaceTempView("smallUser")
        smallUser.cache()
        constituency.createOrReplaceTempView("constituency")
        constituency.cache()
        region.createOrReplaceTempView("region")
        region.cache()
        val newPostcodes = udf((i: String) => i.toLowerCase.replaceAll("\\s", "")).apply(col("Postcode"))
        val newRegions = udf((i: String) => i.toLowerCase.replaceAll("\\s", "-")).apply(col("ScottishParliamentaryRegion2014Name"))
        val newConstituencies = udf((i: String) => i.toLowerCase.replaceAll("\\s", "-")).apply(col("ScottishParliamentaryConstituency2014Name"))
        smallUser
            .join(constituency, constituency.col("ScottishParliamentaryConstituency2014Code") === smallUser.col("ScottishParliamentaryConstituency2014Code"))
            .join(region, region.col("ScottishParliamentaryRegion2014Code") === smallUser.col("ScottishParliamentaryRegion2014Code"))
            .select(smallUser.col("Postcode"), region.col("ScottishParliamentaryRegion2014Name"), constituency.col("ScottishParliamentaryConstituency2014Name"))
            .withColumn("Postcode", newPostcodes)
            .withColumn("ScottishParliamentaryRegion2014Name", newRegions)
            .withColumn("ScottishParliamentaryConstituency2014Name", newConstituencies)
            .coalesce(1)
            .write
            .option("header", "true")
            .option("sep", ",")
            .mode("overwrite")
            .csv("output")
        spark.stop() 
    }
}