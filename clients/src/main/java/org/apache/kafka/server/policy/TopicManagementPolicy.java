/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.server.policy;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Map;

/**
 * A policy that is enforced on topic creation, alteration and deletion,
 * and for the deletion of messages from a topic.
 *
 * An implementation of this policy can be configured on a broker via the
 * {@code topic.management.policy.class.name} broker config.
 * When this is configured the named class will be instantiated reflectively
 * using its nullary constructor and will then pass the broker configs to
 * its <code>configure()</code> method. During broker shutdown, the
 * <code>close()</code> method will be invoked so that resources can be
 * released (if necessary).
 *
 */
public interface TopicManagementPolicy  extends Configurable, AutoCloseable {

    /**
     * Metadata common to all requests about topics.
     */
    interface AbstractRequestMetadata {

        /**
         * The topic the action is being performed upon.
         */
        String topic();

        /**
         * The authenticated principal making the request, or null if the session is not authenticated.
         */
        KafkaPrincipal principal();
    }

    /** Represents a request to create a topic. */
    interface CreateTopicRequest extends AbstractRequestMetadata {
        /**
         * The requested state of the topic to be created.
         */
        RequestTopicState requestedState();
    }

    /**
     * Validate the given request to create a topic
     * and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the request does not satisfy this policy.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
     * Note that validation failure only affects the relevant topic,
     * other topics in the request will still be processed.
     *
     * @param requestMetadata the request parameters for the provided topic.
     * @param clusterState the current state of the cluster
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validateCreateTopic(CreateTopicRequest requestMetadata, ClusterState clusterState)
            throws PolicyViolationException;

    /** Represents a request to alter an existing topic. */
    interface AlterTopicRequest extends AbstractRequestMetadata {
        /**
         * The state the topic will have after the alteration.
         */
        RequestTopicState requestedState();
    }

    /**
     * Validate the given request to alter an existing topic
     * and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the request does not satisfy this policy.
     *
     * The given {@code clusterState} can be used to discover the current state of the topic to be modified.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
     * Note that validation failure only affects the relevant topic,
     * other topics in the request will still be processed.
     *
     * @param requestMetadata the request parameters for the provided topic.
     * @param clusterState the current state of the cluster
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validateAlterTopic(AlterTopicRequest requestMetadata, ClusterState clusterState)
            throws PolicyViolationException;

    /** Represents a request to delete an existing topic. */
    interface DeleteTopicRequest extends AbstractRequestMetadata {
    }

    /**
     * Validate the given request to delete a topic
     * and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the request does not satisfy this policy.
     *
     * The given {@code clusterState} can be used to discover the current state of the topic to be deleted.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
     * Note that validation failure only affects the relevant topic,
     * other topics in the request will still be processed.
     *
     * @param requestMetadata the request parameters for the provided topic.
     * @param clusterState the current state of the cluster
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validateDeleteTopic(DeleteTopicRequest requestMetadata, ClusterState clusterState)
            throws PolicyViolationException;

    /** Represents a request to delete some records from an existing topic. */
    interface DeleteRecordsRequest extends AbstractRequestMetadata {

        /**
         * Returns a map of topic partitions and the corresponding offset of the last message
         * to be retained. Messages before this offset will be deleted.
         * Partitions which won't have messages deleted won't be present in the map.
         */
        Map<Integer, Long> deletedMessageOffsets();
    }

    /**
     * Validate the given request to delete records from a topic
     * and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the request does not satisfy this policy.
     *
     * The given {@code clusterState} can be used to discover the current state of the topic to have records deleted.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
     * Note that validation failure only affects the relevant topic,
     * other topics in the request will still be processed.
     *
     * @param requestMetadata the request parameters for the provided topic.
     * @param clusterState the current state of the cluster
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validateDeleteRecords(DeleteRecordsRequest requestMetadata, ClusterState clusterState)
            throws PolicyViolationException;
}


