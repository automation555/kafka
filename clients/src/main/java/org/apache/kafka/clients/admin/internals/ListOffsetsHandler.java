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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.protocol.Errors.LEADER_NOT_AVAILABLE;

public class ListOffsetsHandler implements AdminApiHandler<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> {
    private final LogContext logContext;
    private final Logger log;
    private final Map<TopicPartition, Long> topicPartitionOffsets;
    private final IsolationLevel isolationLevel;

    public ListOffsetsHandler(
        Map<TopicPartition, Long> topicPartitionOffsets,
        LogContext logContext,
        IsolationLevel isolationLevel
    ) {
        this.topicPartitionOffsets = Collections.unmodifiableMap(topicPartitionOffsets);
        this.log = logContext.logger(ListOffsetsHandler.class);
        this.logContext = logContext;
        this.isolationLevel = isolationLevel;
    }

    @Override
    public String apiName() {
        return "listOffsets";
    }

    @Override
    public Keys<TopicPartition> initializeKeys() {
        PartitionLeaderStrategy lookupStrategy = new PartitionLeaderStrategy(logContext);
        return Keys.dynamicMapped(topicPartitionOffsets.keySet(), lookupStrategy);
    }

    @Override
    public ListOffsetsRequest.Builder buildRequest(
        Integer brokerId,
        Set<TopicPartition> topicPartitions
    ) {

        final ArrayList<ListOffsetsRequestData.ListOffsetsTopic> listOffsetsTopics = new ArrayList<>(CollectionUtils.groupTopicPartitionsByTopic(
            topicPartitions,
            topic -> new ListOffsetsRequestData.ListOffsetsTopic().setName(topic),
            (topicRequest, tp) -> topicRequest.partitions()
                .add(new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(tp.partition())
                    .setTimestamp(topicPartitionOffsets.get(tp))
                )
        ).values());

        return ListOffsetsRequest.Builder
            .forConsumer(true, isolationLevel)
            .setTargetTimes(listOffsetsTopics);
    }

    private void handlePartitionError(
        TopicPartition topicPartition,
        ApiError apiError,
        Map<TopicPartition, Throwable> failed,
        List<TopicPartition> unmapped
    ) {
        Errors error = Errors.forCode(apiError.error().code());
        if (error.exception() instanceof InvalidMetadataException) {
            log.debug("Invalid metadata error in `ListOffsets` response for partition {}. " +
                "Will retry later.", topicPartition);
            if (error == Errors.NOT_LEADER_OR_FOLLOWER || error == LEADER_NOT_AVAILABLE)
                unmapped.add(topicPartition);
        } else {
            log.error("Unexpected error in `ListOffsets` response for partition {}",
                topicPartition, apiError.exception());
            failed.put(topicPartition, apiError.error().exception("Failed to list offsets " +
                "for partition " + topicPartition + " due to unexpected error"));
        }
    }

    @Override
    public ApiResult<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> handleResponse(
        Integer brokerId,
        Set<TopicPartition> keys,
        AbstractResponse abstractResponse
    ) {
        ListOffsetsResponse response = (ListOffsetsResponse) abstractResponse;
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> completed = new HashMap<>();
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        List<TopicPartition> unmapped = new ArrayList<>();

        for (ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse : response.data().topics()) {
            for (ListOffsetsResponseData.ListOffsetsPartitionResponse partitionResponse : topicResponse.partitions()) {
                TopicPartition topicPartition = new TopicPartition(
                    topicResponse.name(), partitionResponse.partitionIndex());

                Errors error = Errors.forCode(partitionResponse.errorCode());
                if (error != Errors.NONE) {
                    ApiError apiError = new ApiError(error);
                    handlePartitionError(topicPartition, apiError, failed, unmapped);
                    continue;
                }

                Optional<Integer> leaderEpoch = (partitionResponse.leaderEpoch() == ListOffsetsResponse.UNKNOWN_EPOCH)
                    ? Optional.empty()
                    : Optional.of(partitionResponse.leaderEpoch());

                ListOffsetsResult.ListOffsetsResultInfo resultInfo = new ListOffsetsResult.ListOffsetsResultInfo(
                    partitionResponse.offset(),
                    partitionResponse.timestamp(),
                    leaderEpoch
                );

                completed.put(topicPartition, resultInfo);
            }
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

}
