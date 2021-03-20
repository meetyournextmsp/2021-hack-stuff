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
        smallUser
            .join(constituency, constituency.col("ScottishParliamentaryConstituency2014Code") === smallUser.col("ScottishParliamentaryConstituency2014Code"))
            .join(region, region.col("ScottishParliamentaryRegion2014Code") === smallUser.col("ScottishParliamentaryRegion2014Code"))
            .select(smallUser.col("Postcode"), region.col("ScottishParliamentaryRegion2014Name"), constituency.col("ScottishParliamentaryConstituency2014Name"))
            .coalesce(1)
            .write
            .option("header", "true")
            .option("sep", ",")
            .mode("overwrite")
            .csv("output")
        spark.stop() 
    }
}