package leongu.myscalaflink.util


import java.util
import java.util.{HashMap, Map}

import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.hbase.HBaseDynamicTableFactory
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.catalog.{CatalogTableImpl, ObjectIdentifier}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.FactoryUtil

object HBaseUtils {

  def createTableSource(schema: TableSchema, options: util.Map[String, String]): DynamicTableSource = {
    FactoryUtil.createTableSource(null,
      ObjectIdentifier.of("default_catalog", "default_catalog", "t1"),
      new CatalogTableImpl(schema, options, "mock source"),
      new Configuration, classOf[HBaseDynamicTableFactory].getClassLoader)
  }

  def createTableSink(schema: TableSchema, options: util.Map[String, String]): DynamicTableSink = {
    FactoryUtil.createTableSink(null,
      ObjectIdentifier.of("default_catalog", "default_catalog", "t1"),
      new CatalogTableImpl(schema, options, "mock sink"),
      new Configuration, classOf[HBaseDynamicTableFactory].getClassLoader)
  }

}
