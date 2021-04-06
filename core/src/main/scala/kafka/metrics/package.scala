/**
 *
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import javax.management.{MalformedObjectNameException, ObjectName}
import com.codahale.metrics._
import com.codahale.metrics.jmx.{JmxReporter, ObjectNameFactory}

import scala.collection.JavaConverters._

package object metrics {

  private[metrics] val kafkaMetricRegistry = new MetricRegistry()

  private val objectNameFactory = new ObjectNameFactory {
    override def createName(typeName: String, domain: String, name: String): ObjectName =
      try {
        new ObjectName(name)
      } catch {
        case _: MalformedObjectNameException => new ObjectName(ObjectName.quote(name))
      }
  }

  private val reporter = JmxReporter.forRegistry(kafkaMetricRegistry).createsObjectNamesWith(objectNameFactory).build()
  reporter.start()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      reporter.stop()
    }
  })

  def getKafkaMetrics: Map[String, Metric] = kafkaMetricRegistry.getMetrics.asScala.toMap

  def removeMetric(metricName: String): Boolean = kafkaMetricRegistry.remove(metricName)

}
