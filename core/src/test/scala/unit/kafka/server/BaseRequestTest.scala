/**
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

package kafka.server

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer
import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.network.SocketServer
import kafka.utils._
import org.apache.kafka.common.ApiKey
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractRequestResponse, RequestHeader, ResponseHeader}

abstract class BaseRequestTest extends KafkaServerTestHarness {
  private var correlationId = 0

  // If required, set number of brokers
  protected def numBrokers: Int = 3

  // If required, override properties by mutating the passed Properties object
  protected def propertyOverrides(properties: Properties) {}

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect,
      enableControlledShutdown = false, enableDeleteTopic = true,
      interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties)
    props.foreach(propertyOverrides)
    props.map(KafkaConfig.fromProps)
  }

  def anySocketServer = {
    servers.find { server =>
      val state = server.brokerState.currentState
      state != NotRunning.state && state != BrokerShuttingDown.state
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No live broker is available"))
  }

  def controllerSocketServer = {
    servers.find { server =>
      server.kafkaController.isActive
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No controller broker is available"))
  }

  def notControllerSocketServer = {
    servers.find { server =>
      !server.kafkaController.isActive
    }.map(_.socketServer).getOrElse(throw new IllegalStateException("No non-controller broker is available"))
  }

  def brokerSocketServer(brokerId: Int) = {
    servers.find { server =>
      server.config.brokerId == brokerId
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"Could not find broker with id $brokerId"))
  }

  def connect(s: SocketServer = anySocketServer, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Socket = {
    new Socket("localhost", s.boundPort(ListenerName.forSecurityProtocol(protocol)))
  }

  private def sendRequest(socket: Socket, request: Array[Byte]) {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

  private def receiveResponse(socket: Socket): Array[Byte] = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  def requestAndReceive(socket: Socket, request: Array[Byte]): Array[Byte] = {
    sendRequest(socket, request)
    receiveResponse(socket)
  }

  /**
    * @param destination An optional SocketServer ot send the request to. If not set, any available server is used.
    * @param protocol An optional SecurityProtocol to use. If not set, PLAINTEXT is used.
    * @return A ByteBuffer containing the response (without the response header)
    */
  def connectAndSend(request: AbstractRequest, api: ApiKey,
                     destination: SocketServer = anySocketServer,
                     apiVersion: Option[Short] = None,
                     protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): ByteBuffer = {
    val socket = connect(destination, protocol)
    try send(request, api, socket, apiVersion)
    finally socket.close()
  }

  /**
    * @param destination An optional SocketServer ot send the request to. If not set, any available server is used.
    * @param protocol An optional SecurityProtocol to use. If not set, PLAINTEXT is used.
    * @return A ByteBuffer containing the response (without the response header).
    */
  def connectAndSendStruct(requestStruct: Struct, api: ApiKey, apiVersion: Short,
                           destination: SocketServer = anySocketServer,
                           protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): ByteBuffer = {
    val socket = connect(destination, protocol)
    try sendStruct(requestStruct, api, socket, apiVersion)
    finally socket.close()
  }

  /**
    * Serializes and sends the request to the given api.
    * A ByteBuffer containing the response is returned.
    */
  def send(request: AbstractRequest, api: ApiKey, socket: Socket, apiVersion: Option[Short] = None): ByteBuffer = {
    val header = nextRequestHeader(api, apiVersion.getOrElse(request.version))
    val serializedBytes = request.serialize(header).array
    val response = requestAndReceive(socket, serializedBytes)
    skipResponseHeader(response)
  }

  /**
    * Serializes and sends the requestStruct to the given api.
    * A ByteBuffer containing the response (without the response header) is returned.
    */
  def sendStruct(requestStruct: Struct, api: ApiKey, socket: Socket, apiVersion: Short): ByteBuffer = {
    val header = nextRequestHeader(api, apiVersion)
    val serializedBytes = AbstractRequestResponse.serialize(header.toStruct, requestStruct).array
    val response = requestAndReceive(socket, serializedBytes)
    skipResponseHeader(response)
  }

  protected def skipResponseHeader(response: Array[Byte]): ByteBuffer = {
    val responseBuffer = ByteBuffer.wrap(response)
    // Parse the header to ensure its valid and move the buffer forward
    ResponseHeader.parse(responseBuffer)
    responseBuffer
  }

  def nextRequestHeader(api: ApiKey, apiVersion: Short): RequestHeader = {
    correlationId += 1
    new RequestHeader(api.id, apiVersion, "client-id", correlationId)
  }

}
