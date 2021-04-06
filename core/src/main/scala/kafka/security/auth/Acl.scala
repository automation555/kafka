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

package kafka.security.auth

import java.nio.charset.StandardCharsets

import kafka.utils.Json
import kafka.utils.json.JsonValue
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.SecurityUtils

import scala.collection.JavaConverters._

object Acl {
  val WildCardPrincipal: KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*")
  val WildCardHost: String = "*"
  val AllowAllAcl = new Acl(WildCardPrincipal, Allow, WildCardHost, All)
  val PrincipalKey = "principal"
  val PermissionTypeKey = "permissionType"
  val OperationKey = "operation"
  val HostsKey = "host"
  val VersionKey = "version"
  val CurrentVersion = 1
  val AclsKey = "acls"

  /**
   *
   * @param bytes of acls json string
   *
   * <p>
      {
        "version": 1,
        "acls": [
          {
            "host":"host1",
            "permissionType": "Deny",
            "operation": "Read",
            "principal": "User:alice"
          }
        ]
      }
   * </p>
   *
   * @return
   */
  def fromBytes(bytes: Array[Byte]): Set[Acl] = {
    if (bytes == null || bytes.isEmpty)
      return collection.immutable.Set.empty[Acl]

    parseBytesWithAclFallback(bytes).map(_.asJsonObject).map { js =>
      //the acl json version.
      require(js(VersionKey).to[Int] == CurrentVersion)
      js(AclsKey).asJsonArray.iterator.map(_.asJsonObject).map { itemJs =>
        val principal = SecurityUtils.parseKafkaPrincipal(itemJs(PrincipalKey).to[String])
        val permissionType = PermissionType.fromString(itemJs(PermissionTypeKey).to[String])
        val host = itemJs(HostsKey).to[String]
        val operation = Operation.fromString(itemJs(OperationKey).to[String])
        new Acl(principal, permissionType, host, operation)
      }.toSet
    }.getOrElse(Set.empty)
  }

  def toJsonCompatibleMap(acls: Set[Acl]): Map[String, Any] = {
    Map(Acl.VersionKey -> Acl.CurrentVersion, Acl.AclsKey -> acls.map(acl => acl.toMap.asJava).toList.asJava)
  }

  /**
    * Parse a JSON string into a JsonValue if possible. `None` is returned if `input` is not valid JSON. This method is currently used
    * to read the already stored invalid ACLs JSON which was persisted using older versions of Kafka (prior to Kafka 1.1.0). KAFKA-6319
    */
  private def parseBytesWithAclFallback(input: Array[Byte]): Option[JsonValue] = {
    // Before 1.0.1, Json#encode did not escape backslash or any other special characters. SSL principals
    // stored in ACLs may contain backslash as an escape char, making the JSON generated in earlier versions invalid.
    // Escape backslash and retry to handle these strings which may have been persisted in ZK.
    // Note that this does not handle all special characters (e.g. non-escaped double quotes are not supported)
    Json.tryParseBytes(input) match {
      case Left(e) =>
        val escapedInput = new String(input, StandardCharsets.UTF_8).replaceAll("\\\\", "\\\\\\\\")
        Json.parseFull(escapedInput)
      case Right(v) => Some(v)
    }
  }
}

/**
 * An instance of this class will represent an acl that can express following statement.
 * <pre>
 * Principal P has permissionType PT on Operation O1 from hosts H1.
 * </pre>
 * @param principal A value of *:* indicates all users.
 * @param permissionType
 * @param host A value of * indicates all hosts.
 * @param operation A value of ALL indicates all operations.
 */
case class Acl(principal: KafkaPrincipal, permissionType: PermissionType, host: String, operation: Operation) {

  /**
   * TODO: Ideally we would have a symmetric toJson method but our current json library can not jsonify/dejsonify complex objects.
   * @return Map representation of the Acl.
   */
  def toMap(): Map[String, Any] = {
    Map(Acl.PrincipalKey -> principal.toString,
      Acl.PermissionTypeKey -> permissionType.name,
      Acl.OperationKey -> operation.name,
      Acl.HostsKey -> host)
  }

  override def toString: String = {
    "%s has %s permission for operations: %s from hosts: %s".format(principal, permissionType.name, operation, host)
  }

}
