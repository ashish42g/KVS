import com.databricks.spark.avro._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object KKVSEncoder extends Constants {
  val MASTER: String = "local[*]"
  var unmappedKeySet: mutable.Set[String] = mutable.Set()

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(KKVSEncoder.getClass.getName).setMaster(MASTER)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    var rank: Long = 0L
    val MAPPING_FILE: String = args(1)

    val tuple: (mutable.Map[String, String], Long) = loadMappingFile(MAPPING_FILE)
    val localMap = sc.broadcast[mutable.Map[String, String]](tuple._1)
    val maxRank = sc.broadcast[Long](tuple._2)
    val df: DataFrame = sqlContext.read.avro(args(0))


    val entityRDD: RDD[Entity] = df.map(x =>
      Entity(
        x.getAs[Long](latest_used_storage_size_dly),
        x.getAs[Long](aggr_count_dly),
        x.getAs[Long](prs_id),
        x.getAs[String](ud_id),
        x.getAs[String](event_type),
        x.getAs[String](platform),
        x.getAs[String](os_version),
        x.getAs[String](application),
        x.getAs[String](bundle_id),
        x.getAs[String](kv_id),
        x.getAs[String](server_error),
        x.getAs[String](service_type),
        x.getAs[String](event_date_1),
        x.getAs[String](event_date_2),
        x.getAs[Long](event_time_epoch),
        empty
      )
    )

    val processedEntity = entityRDD.map(x => processEntity(x, localMap)).map(x => (x.TYPE, x))

    processedEntity.map(x =>
      (x._2.TYPE,
        (
          x._2.LATEST_USED_STORAGE_SIZE_DLY,
          x._2.AGGR_COUNT_DLY,
          x._2.PRS_ID,
          x._2.UD_ID,
          x._2.EVENT_TYPE,
          x._2.PLATFORM,
          x._2.OS_VERSION,
          x._2.APPLICATION,
          x._2.BUNDLE_ID,
          x._2.KV_ID,
          x._2.SERVER_ERROR,
          x._2.SERVICE_TYPE,
          x._2.EVENT_DATE_1,
          x._2.EVENT_DATE_2,
          x._2.EVENT_TIME_EPOCH
          )
        )
    ).toDF("Type", "Entity").write.partitionBy("Type").avro(args(2))

    def computeRank(): Long = {
      if (rank == 0) {
        rank = maxRank.value + 1
        rank
      } else
        rank + 1
    }

    val mappedRDD = sc.parallelize(unmappedKeySet.toSeq).map(x => (x, maxRank.value)).distinct.repartition(1).mapValues(x => computeRank())
    mappedRDD.saveAsTextFile(args(3))
    val finalMap = sc.broadcast(mappedRDD.collectAsMap)

    mappedRDD.collectAsMap.foreach(println)

    val partialEncodedData = processedEntity.filter(x => x._1.equals(partial_encoded_data))
    partialEncodedData.foreach(println)

    val fullEncodedData = partialEncodedData.map(x => finalProcessing(x._2, finalMap))

    processedEntity.map(x => if (x._1.equals(partial_encoded_data)) finalProcessing(x._2, finalMap) else x._2)

    fullEncodedData.map(x =>
      (x.TYPE,
        (
          x.LATEST_USED_STORAGE_SIZE_DLY,
          x.AGGR_COUNT_DLY,
          x.PRS_ID,
          x.UD_ID,
          x.EVENT_TYPE,
          x.PLATFORM,
          x.OS_VERSION,
          x.APPLICATION,
          x.BUNDLE_ID,
          x.KV_ID,
          x.SERVER_ERROR,
          x.SERVICE_TYPE,
          x.EVENT_DATE_1,
          x.EVENT_DATE_2,
          x.EVENT_TIME_EPOCH
          )
        )
    ).toDF("Type", "Entity").write.partitionBy("Type").avro(args(4))

  }


  def loadMappingFile(mappingFile: String) = {

    val localMap = mutable.Map("" -> "")
    var rank: Long = 0L
    var maxRank: Long = 0L

    for (line <- Source.fromFile(mappingFile).getLines) {
      if (line != null) {
        val mappingFields: Array[String] = line.split("\t")
        if (mappingFields.length > 1) {
          localMap.put(mappingFields(1), mappingFields(0))
          rank = mappingFields(0).toLong
          if (maxRank < rank || maxRank == 0) {
            maxRank = rank
          }
        }
      }
    }
    (localMap, maxRank)
  }


  def extractFromLocalMap(field: String, localMap: mutable.Map[String, String]): (String, Boolean) = {
    val mappedValue: Option[String] = localMap.get(field)

    if (mappedValue.isDefined) {
      if (mappedValue.size < field.length) {
        (mappedValue.get, true)
      } else {
        (field, true)
      }
    } else {
      (field, false)
    }
  }


  def processEntity(entity: Entity, localMap: Broadcast[mutable.Map[String, String]]) = {

    def AddToUnmappedSet(value: String): mutable.Set[String] = {
      unmappedKeySet += value
    }

    if (entity.EVENT_TYPE != null && entity.EVENT_TYPE.equalsIgnoreCase("PUT")) {
      entity.EVENT_TYPE = "1"
    } else if (entity.EVENT_TYPE != null && entity.EVENT_TYPE.equalsIgnoreCase("GET")) {
      entity.EVENT_TYPE = "0"
    }

    val bundleId = extractFromLocalMap(entity.BUNDLE_ID, localMap.value)
    val osVersion = extractFromLocalMap(entity.OS_VERSION, localMap.value)
    val platform = extractFromLocalMap(entity.PLATFORM, localMap.value)
    val application = extractFromLocalMap(entity.APPLICATION, localMap.value)
    val kvId = extractFromLocalMap(entity.KV_ID, localMap.value)

    entity.BUNDLE_ID = bundleId._1
    entity.OS_VERSION = osVersion._1
    entity.PLATFORM = platform._1
    entity.APPLICATION = application._1
    entity.KV_ID = kvId._1

    if (!bundleId._2) {
      AddToUnmappedSet(bundleId._1)
      entity.TYPE = partial_encoded_data
    }
    if (!osVersion._2) {
      AddToUnmappedSet(osVersion._1)
      entity.TYPE = partial_encoded_data
    }
    if (!platform._2) {
      AddToUnmappedSet(platform._1)
      entity.TYPE = partial_encoded_data
    }
    if (!application._2) {
      AddToUnmappedSet(application._1)
      entity.TYPE = partial_encoded_data
    }
    if (!kvId._2) {
      AddToUnmappedSet(kvId._1)
      entity.TYPE = partial_encoded_data
    }
    if (!entity.TYPE.equalsIgnoreCase(partial_encoded_data)) {
      entity.TYPE = initial_encoded_data
    }
    entity
  }


  def extractFromFinalMap(field: String, finalMap: scala.collection.Map[String, Long]): String = {
    val mappedValue: Option[Long] = finalMap.get(field)

    if (mappedValue.isDefined) {
      if (mappedValue.size < field.length) {
        mappedValue.get.toString
      } else {
        field
      }
    } else {
      field
    }
  }


  def finalProcessing(entity: Entity, finalMap: Broadcast[scala.collection.Map[String, Long]]) = {

    val bundleId = extractFromFinalMap(entity.BUNDLE_ID, finalMap.value)
    val osVersion = extractFromFinalMap(entity.OS_VERSION, finalMap.value)
    val platform = extractFromFinalMap(entity.PLATFORM, finalMap.value)
    val application = extractFromFinalMap(entity.APPLICATION, finalMap.value)
    val kvId = extractFromFinalMap(entity.KV_ID, finalMap.value)

    entity.BUNDLE_ID = bundleId
    entity.OS_VERSION = osVersion
    entity.PLATFORM = platform
    entity.APPLICATION = application
    entity.KV_ID = kvId
    entity.TYPE = full_encoded_data

    entity
  }

}
