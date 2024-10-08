/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventWithPreSchema;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.runtime.operators.schema.event.RefreshPendingListsResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.ReleaseUpstreamRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.ReleaseUpstreamResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistryRequestHandler.RequestStatus.RECEIVED_RELEASE_REQUEST;
import static org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

/** A handler to deal with all requests and events for {@link SchemaRegistry}. */
@Internal
@NotThreadSafe
public class SchemaRegistryRequestHandler implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryRequestHandler.class);

    /** The {@link MetadataApplier} for every table. */
    private final MetadataApplier metadataApplier;
    /** All active sink writers. */
    private final Set<Integer> activeSinkWriters;
    /** Schema manager holding schema for all tables. */
    private final SchemaManager schemaManager;

    private final SchemaDerivation schemaDerivation;

    /**
     * Not applied SchemaChangeRequest before receiving all flush success events for its table from
     * sink writers.
     */
    private final List<PendingSchemaChange> pendingSchemaChanges;

    private final List<SchemaChangeEvent> finishedSchemaChanges;
    private final List<Tuple2<SchemaChangeEvent, Throwable>> failedSchemaChanges;
    private final List<SchemaChangeEvent> ignoredSchemaChanges;

    /** Sink writers which have sent flush success events for the request. */
    private final Set<Integer> flushedSinkWriters;

    /** Status of the execution of current schema change request. */
    private volatile boolean isSchemaChangeApplying;
    /** Executor service to execute schema change. */
    private final ExecutorService schemaChangeThreadPool;

    private final SchemaChangeBehavior schemaChangeBehavior;

    public SchemaRegistryRequestHandler(
            MetadataApplier metadataApplier,
            SchemaManager schemaManager,
            SchemaDerivation schemaDerivation,
            SchemaChangeBehavior schemaChangeBehavior) {
        this.metadataApplier = metadataApplier;
        this.activeSinkWriters = new HashSet<>();
        this.flushedSinkWriters = new HashSet<>();
        this.pendingSchemaChanges = new LinkedList<>();
        this.finishedSchemaChanges = new LinkedList<>();
        this.failedSchemaChanges = new LinkedList<>();
        this.ignoredSchemaChanges = new LinkedList<>();
        this.schemaManager = schemaManager;
        this.schemaDerivation = schemaDerivation;
        this.schemaChangeThreadPool = Executors.newSingleThreadExecutor();
        this.isSchemaChangeApplying = false;
        this.schemaChangeBehavior = schemaChangeBehavior;
    }

    /**
     * Apply the schema change to the external system.
     *
     * @param tableId the table need to change schema
     * @param derivedSchemaChangeEvents list of the schema changes
     */
    private void applySchemaChange(
            TableId tableId, List<SchemaChangeEvent> derivedSchemaChangeEvents) {
        isSchemaChangeApplying = true;
        finishedSchemaChanges.clear();
        failedSchemaChanges.clear();
        ignoredSchemaChanges.clear();

        for (SchemaChangeEvent changeEvent : derivedSchemaChangeEvents) {
            if (changeEvent.getType() != SchemaChangeEventType.CREATE_TABLE) {
                if (schemaChangeBehavior == SchemaChangeBehavior.IGNORE) {
                    ignoredSchemaChanges.add(changeEvent);
                    continue;
                }
            }
            if (!metadataApplier.acceptsSchemaEvolutionType(changeEvent.getType())) {
                LOG.info("Ignored schema change {} to table {}.", changeEvent, tableId);
                ignoredSchemaChanges.add(changeEvent);
            } else {
                try {
                    metadataApplier.applySchemaChange(changeEvent);
                    LOG.debug("Applied schema change {} to table {}.", changeEvent, tableId);
                    finishedSchemaChanges.add(changeEvent);
                } catch (SchemaEvolveException e) {
                    LOG.error(
                            "Failed to apply schema change {} to table {}. Caused by: {}",
                            changeEvent,
                            tableId,
                            e);
                    failedSchemaChanges.add(Tuple2.of(changeEvent, e));
                }
            }
        }
        PendingSchemaChange waitFlushSuccess = pendingSchemaChanges.get(0);
        if (RECEIVED_RELEASE_REQUEST.equals(waitFlushSuccess.getStatus())) {
            startNextSchemaChangeRequest();
        }
        isSchemaChangeApplying = false;
    }

    /**
     * Handle the {@link SchemaChangeRequest} and wait for all sink subtasks flushing.
     *
     * @param request the received SchemaChangeRequest
     */
    public CompletableFuture<CoordinationResponse> handleSchemaChangeRequest(
            SchemaChangeRequest request) {
        if (pendingSchemaChanges.isEmpty()) {
            LOG.info(
                    "Received schema change event request from table {}. Start to buffer requests for others.",
                    request.getTableId().toString());
            SchemaChangeEvent event = request.getSchemaChangeEvent();
            if (event instanceof CreateTableEvent
                    && schemaManager.originalSchemaExists(request.getTableId())) {
                return CompletableFuture.completedFuture(
                        wrap(new SchemaChangeResponse(Collections.emptyList())));
            }
            schemaManager.applyOriginalSchemaChange(event);
            List<SchemaChangeEvent> derivedSchemaChangeEvents =
                    calculateDerivedSchemaChangeEvents(request.getSchemaChangeEvent());
            derivedSchemaChangeEvents.forEach(
                    e -> {
                        if (e instanceof SchemaChangeEventWithPreSchema) {
                            SchemaChangeEventWithPreSchema pe = (SchemaChangeEventWithPreSchema) e;
                            if (!pe.hasPreSchema()) {
                                schemaManager
                                        .getLatestEvolvedSchema(pe.tableId())
                                        .ifPresent(pe::fillPreSchema);
                            }
                        }
                    });
            CompletableFuture<CoordinationResponse> response =
                    CompletableFuture.completedFuture(
                            wrap(new SchemaChangeResponse(derivedSchemaChangeEvents)));
            if (!derivedSchemaChangeEvents.isEmpty()) {
                PendingSchemaChange pendingSchemaChange =
                        new PendingSchemaChange(request, response);
                pendingSchemaChange.derivedSchemaChangeEvents = derivedSchemaChangeEvents;
                pendingSchemaChanges.add(pendingSchemaChange);
                pendingSchemaChanges.get(0).startToWaitForReleaseRequest();
            }
            return response;
        } else {
            LOG.info("There are already processing requests. Wait for processing.");
            CompletableFuture<CoordinationResponse> response = new CompletableFuture<>();
            pendingSchemaChanges.add(new PendingSchemaChange(request, response));
            return response;
        }
    }

    /** Handle the {@link ReleaseUpstreamRequest} and wait for all sink subtasks flushing. */
    public CompletableFuture<CoordinationResponse> handleReleaseUpstreamRequest() {
        CompletableFuture<CoordinationResponse> response =
                pendingSchemaChanges.get(0).getResponseFuture();
        if (response.isDone() && !isSchemaChangeApplying) {
            startNextSchemaChangeRequest();
        } else {
            pendingSchemaChanges.get(0).receiveReleaseRequest();
        }
        return response;
    }

    /**
     * Register a sink subtask.
     *
     * @param sinkSubtask the sink subtask to register
     */
    public void registerSinkWriter(int sinkSubtask) {
        LOG.info("Register sink subtask {}.", sinkSubtask);
        activeSinkWriters.add(sinkSubtask);
    }

    /**
     * Record flushed sink subtasks after receiving FlushSuccessEvent.
     *
     * @param tableId the subtask in SchemaOperator and table that the FlushEvent is about
     * @param sinkSubtask the sink subtask succeed flushing
     */
    public void flushSuccess(TableId tableId, int sinkSubtask) throws InterruptedException {
        flushedSinkWriters.add(sinkSubtask);
        if (flushedSinkWriters.equals(activeSinkWriters)) {
            LOG.info(
                    "All sink subtask have flushed for table {}. Start to apply schema change.",
                    tableId.toString());
            PendingSchemaChange waitFlushSuccess = pendingSchemaChanges.get(0);
            schemaChangeThreadPool.submit(
                    () -> applySchemaChange(tableId, waitFlushSuccess.derivedSchemaChangeEvents));
            Thread.sleep(1000);

            if (isSchemaChangeApplying) {
                waitFlushSuccess
                        .getResponseFuture()
                        .complete(wrap(new SchemaChangeProcessingResponse()));
            } else {
                waitFlushSuccess
                        .getResponseFuture()
                        .complete(
                                wrap(
                                        new ReleaseUpstreamResponse(
                                                finishedSchemaChanges,
                                                failedSchemaChanges,
                                                ignoredSchemaChanges)));
            }
        }
    }

    private void startNextSchemaChangeRequest() {
        pendingSchemaChanges.remove(0);
        flushedSinkWriters.clear();
        while (!pendingSchemaChanges.isEmpty()) {
            PendingSchemaChange pendingSchemaChange = pendingSchemaChanges.get(0);
            SchemaChangeRequest request = pendingSchemaChange.changeRequest;
            if (request.getSchemaChangeEvent() instanceof CreateTableEvent
                    && schemaManager.evolvedSchemaExists(request.getTableId())) {
                pendingSchemaChange
                        .getResponseFuture()
                        .complete(wrap(new SchemaChangeResponse(Collections.emptyList())));
                pendingSchemaChanges.remove(0);
            } else {
                List<SchemaChangeEvent> derivedSchemaChangeEvents =
                        calculateDerivedSchemaChangeEvents(request.getSchemaChangeEvent());
                pendingSchemaChange
                        .getResponseFuture()
                        .complete(wrap(new SchemaChangeResponse(derivedSchemaChangeEvents)));
                if (!derivedSchemaChangeEvents.isEmpty()) {
                    pendingSchemaChange.derivedSchemaChangeEvents = derivedSchemaChangeEvents;
                    pendingSchemaChange.startToWaitForReleaseRequest();
                    break;
                }
            }
        }
    }

    public CompletableFuture<CoordinationResponse> refreshPendingLists() {
        pendingSchemaChanges.clear();
        flushedSinkWriters.clear();
        return CompletableFuture.completedFuture(wrap(new RefreshPendingListsResponse()));
    }

    public CompletableFuture<CoordinationResponse> getSchemaChangeResult() {
        if (isSchemaChangeApplying) {
            return CompletableFuture.supplyAsync(() -> wrap(new SchemaChangeProcessingResponse()));
        } else {
            return CompletableFuture.supplyAsync(
                    () ->
                            wrap(
                                    new ReleaseUpstreamResponse(
                                            finishedSchemaChanges,
                                            failedSchemaChanges,
                                            ignoredSchemaChanges)));
        }
    }

    @Override
    public void close() throws IOException {
        if (schemaChangeThreadPool != null) {
            schemaChangeThreadPool.shutdown();
        }
    }

    private List<SchemaChangeEvent> calculateDerivedSchemaChangeEvents(SchemaChangeEvent event) {
        if (SchemaChangeBehavior.LENIENT.equals(schemaChangeBehavior)) {
            return lenientizeSchemaChangeEvent(event).stream()
                    .flatMap(evt -> schemaDerivation.applySchemaChange(evt).stream())
                    .collect(Collectors.toList());
        } else {
            return schemaDerivation.applySchemaChange(event);
        }
    }

    private List<SchemaChangeEvent> lenientizeSchemaChangeEvent(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            return Collections.singletonList(event);
        }
        TableId tableId = event.tableId();
        Schema evolvedSchema =
                schemaManager
                        .getLatestEvolvedSchema(tableId)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Evolved schema does not exist, not ready for schema change event "
                                                        + event));
        switch (event.getType()) {
            case ADD_COLUMN:
                {
                    AddColumnEvent addColumnEvent = (AddColumnEvent) event;
                    return Collections.singletonList(
                            new AddColumnEvent(
                                    tableId,
                                    addColumnEvent.getAddedColumns().stream()
                                            .map(
                                                    col ->
                                                            new AddColumnEvent.ColumnWithPosition(
                                                                    Column.physicalColumn(
                                                                            col.getAddColumn()
                                                                                    .getName(),
                                                                            col.getAddColumn()
                                                                                    .getType()
                                                                                    .nullable(),
                                                                            col.getAddColumn()
                                                                                    .getComment())))
                                            .collect(Collectors.toList())));
                }
            case DROP_COLUMN:
                {
                    DropColumnEvent dropColumnEvent = (DropColumnEvent) event;
                    Map<String, DataType> convertNullableColumns =
                            dropColumnEvent.getDroppedColumnNames().stream()
                                    .map(evolvedSchema::getColumn)
                                    .flatMap(e -> e.map(Stream::of).orElse(Stream.empty()))
                                    .filter(col -> !col.getType().isNullable())
                                    .collect(
                                            Collectors.toMap(
                                                    Column::getName,
                                                    column -> column.getType().nullable()));

                    if (convertNullableColumns.isEmpty()) {
                        return Collections.emptyList();
                    } else {
                        return Collections.singletonList(
                                new AlterColumnTypeEvent(tableId, convertNullableColumns));
                    }
                }
            case RENAME_COLUMN:
                {
                    RenameColumnEvent renameColumnEvent = (RenameColumnEvent) event;
                    List<AddColumnEvent.ColumnWithPosition> appendColumns = new ArrayList<>();
                    Map<String, DataType> convertNullableColumns = new HashMap<>();
                    renameColumnEvent
                            .getNameMapping()
                            .forEach(
                                    (key, value) -> {
                                        Column column =
                                                evolvedSchema
                                                        .getColumn(key)
                                                        .orElseThrow(
                                                                () ->
                                                                        new IllegalArgumentException(
                                                                                "Non-existed column "
                                                                                        + key
                                                                                        + " in evolved schema."));
                                        if (!column.getType().isNullable()) {
                                            // It's a not-nullable column, we need to cast it to
                                            // nullable first
                                            convertNullableColumns.put(
                                                    key, column.getType().nullable());
                                        }
                                        appendColumns.add(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                value,
                                                                column.getType().nullable(),
                                                                column.getComment())));
                                    });

                    List<SchemaChangeEvent> events = new ArrayList<>();
                    events.add(new AddColumnEvent(tableId, appendColumns));
                    if (!convertNullableColumns.isEmpty()) {
                        events.add(new AlterColumnTypeEvent(tableId, convertNullableColumns));
                    }
                    return events;
                }
            case DROP_TABLE:
                // We don't drop any tables in Lenient mode.
                LOG.info("A drop table event {} has been ignored in Lenient mode.", event);
                return Collections.emptyList();
            default:
                return Collections.singletonList(event);
        }
    }

    private static class PendingSchemaChange {
        private final SchemaChangeRequest changeRequest;
        private List<SchemaChangeEvent> derivedSchemaChangeEvents;
        private CompletableFuture<CoordinationResponse> responseFuture;
        private RequestStatus status;

        public PendingSchemaChange(
                SchemaChangeRequest changeRequest,
                CompletableFuture<CoordinationResponse> responseFuture) {
            this.changeRequest = changeRequest;
            this.responseFuture = responseFuture;
            this.status = RequestStatus.PENDING;
        }

        public SchemaChangeRequest getChangeRequest() {
            return changeRequest;
        }

        public CompletableFuture<CoordinationResponse> getResponseFuture() {
            return responseFuture;
        }

        public RequestStatus getStatus() {
            return status;
        }

        public void startToWaitForReleaseRequest() {
            if (!responseFuture.isDone()) {
                throw new IllegalStateException(
                        "Cannot start to wait for flush success before the SchemaChangeRequest is done.");
            }
            this.responseFuture = new CompletableFuture<>();
            this.status = RequestStatus.WAIT_RELEASE_REQUEST;
        }

        public void receiveReleaseRequest() {
            this.status = RECEIVED_RELEASE_REQUEST;
        }
    }

    enum RequestStatus {
        PENDING,
        WAIT_RELEASE_REQUEST,
        RECEIVED_RELEASE_REQUEST
    }
}
