/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.spi.v1;

import io.grpc.ConnectivityState;
import io.grpc.Status;
import io.opencensus.metrics.DerivedLongCumulative;
import io.opencensus.metrics.DerivedLongGauge;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.LabelValue;
import io.opencensus.metrics.MetricOptions;
import io.opencensus.metrics.MetricRegistry;
import io.opencensus.metrics.Metrics;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ChannelPoolMonitor {
  private final AtomicInteger channelCounter = new AtomicInteger();
  private final int maxAllowedChannels;
  private static final String prefix = "tmp/gax-pool/";
  private static final String COUNT = "1";
  private static final String MICROSECOND = "us";
  private static final List<LabelKey> labelKeys = Collections.singletonList(
      LabelKey.create("gax_pool_idx", "GAX channel pool index"));
  private static final List<LabelKey> labelKeysWithResult = new ArrayList<>();
  static {
    labelKeysWithResult.addAll(labelKeys);
    labelKeysWithResult.add(LabelKey.create("result", "RPC call result"));
  }
  private final List<LabelValue> labelValues;
  private final List<LabelValue> labelValuesSuccess;
  private final List<LabelValue> labelValuesError;

  private final List<Long> notReadySince = new ArrayList<>();
  private final List<AtomicInteger> activeStreams = new ArrayList<>();
  private final AtomicInteger totalActiveStreams = new AtomicInteger();
  private final List<AtomicInteger> okCalls = new ArrayList<>();
  private final List<AtomicInteger> errCalls = new ArrayList<>();
  private int minActiveStreams = 0;
  private int maxActiveStreams = 0;
  private int minTotalActiveStreams = 0;
  private int maxTotalActiveStreams = 0;
  private final List<AtomicInteger> sessions = new ArrayList<>();
  private int minSessions = 0;
  private int maxSessions = 0;
  private final AtomicInteger sessionsCreated = new AtomicInteger();
  private final AtomicInteger readyChannels = new AtomicInteger();
  private int minReadyChannels = 0;
  private int maxReadyChannels = 0;
  private long minReadinessTime = 0;
  private long avgReadinessTime = 0;
  private long maxReadinessTime = 0;
  private final AtomicLong totalReadinessTime = new AtomicLong();
  private final AtomicLong readinessTimeOccurrences = new AtomicLong();
  private final AtomicLong numChannelConnect = new AtomicLong();
  private final AtomicLong numChannelDisconnect = new AtomicLong();
  private int minOkCalls = 0;
  private int maxOkCalls = 0;
  private final AtomicLong totalOkCalls = new AtomicLong();
  private boolean minOkReported = false;
  private boolean maxOkReported = false;
  private int minErrCalls = 0;
  private int maxErrCalls = 0;
  private final AtomicLong totalErrCalls = new AtomicLong();
  private boolean minErrReported = false;
  private boolean maxErrReported = false;

  public ChannelPoolMonitor(int poolIndex, int maxAllowedChannels) {
    this.maxAllowedChannels = maxAllowedChannels;
    labelValues = Collections.singletonList(LabelValue.create(String.format("pool-%d", poolIndex)));
    labelValuesSuccess = new ArrayList<>();
    labelValuesSuccess.addAll(labelValues);
    labelValuesSuccess.add(LabelValue.create("SUCCESS"));
    labelValuesError = new ArrayList<>();
    labelValuesError.addAll(labelValues);
    labelValuesError.add(LabelValue.create("ERROR"));
    initMetrics();
  }

  private void initMetrics() {
    final MetricRegistry registry = Metrics.getMetricRegistry();

    final DerivedLongGauge minReadyChannels =
        registry.addDerivedLongGauge(
            prefix + "min_ready_channels",
            MetricOptions.builder()
                .setDescription("The minimum number of channels simultaneously in the READY state over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    minReadyChannels.removeTimeSeries(labelValues);
    minReadyChannels.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMinReadyChannels);

    final DerivedLongGauge maxReadyChannels =
        registry.addDerivedLongGauge(
            prefix + "max_ready_channels",
            MetricOptions.builder()
                .setDescription("The maximum number of channels simultaneously in the READY state over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    maxReadyChannels.removeTimeSeries(labelValues);
    maxReadyChannels.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMaxReadyChannels);

    final DerivedLongGauge maxChannels =
        registry.addDerivedLongGauge(
            prefix + "max_channels",
            MetricOptions.builder()
                .setDescription("The maximum number of existing channels over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    maxChannels.removeTimeSeries(labelValues);
    maxChannels.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMaxChannels);

    final DerivedLongGauge maxAllowedChannels =
        registry.addDerivedLongGauge(
            prefix + "max_allowed_channels",
            MetricOptions.builder()
                .setDescription("The maximum number of channels allowed in the pool. (The poll max size)")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    maxAllowedChannels.removeTimeSeries(labelValues);
    maxAllowedChannels.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMaxAllowedChannels);

    final DerivedLongCumulative numChannelDisconnect =
        registry.addDerivedLongCumulative(
            prefix + "num_channel_disconnect",
            MetricOptions.builder()
                .setDescription("The number of disconnections (occurrences when a channel deviates from the READY state)")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    numChannelDisconnect.removeTimeSeries(labelValues);
    numChannelDisconnect.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportNumChannelDisconnect);

    final DerivedLongCumulative numChannelConnect =
        registry.addDerivedLongCumulative(
            prefix + "num_channel_connect",
            MetricOptions.builder()
                .setDescription("The number of times when a channel reached the READY state.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    numChannelConnect.removeTimeSeries(labelValues);
    numChannelConnect.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportNumChannelConnect);

    final DerivedLongGauge minChannelReadinessTime =
        registry.addDerivedLongGauge(
            prefix + "min_channel_readiness_time",
            MetricOptions.builder()
                .setDescription("The minimum time it took to transition a channel to the READY state over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(MICROSECOND)
                .build());

    minChannelReadinessTime.removeTimeSeries(labelValues);
    minChannelReadinessTime.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMinReadinessTime);

    final DerivedLongGauge avgChannelReadinessTime =
        registry.addDerivedLongGauge(
            prefix + "avg_channel_readiness_time",
            MetricOptions.builder()
                .setDescription("The average time it took to transition a channel to the READY state over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(MICROSECOND)
                .build());

    avgChannelReadinessTime.removeTimeSeries(labelValues);
    avgChannelReadinessTime.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportAvgReadinessTime);

    final DerivedLongGauge maxChannelReadinessTime =
        registry.addDerivedLongGauge(
            prefix + "max_channel_readiness_time",
            MetricOptions.builder()
                .setDescription("The maximum time it took to transition a channel to the READY state over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(MICROSECOND)
                .build());

    maxChannelReadinessTime.removeTimeSeries(labelValues);
    maxChannelReadinessTime.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMaxReadinessTime);

    final DerivedLongGauge minActiveStreams =
        registry.addDerivedLongGauge(
            prefix + "min_active_streams_per_channel",
            MetricOptions.builder()
                .setDescription("The minimum number of active streams on any channel over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    minActiveStreams.removeTimeSeries(labelValues);
    minActiveStreams.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMinActiveStreams);

    final DerivedLongGauge maxActiveStreams =
        registry.addDerivedLongGauge(
            prefix + "max_active_streams_per_channel",
            MetricOptions.builder()
                .setDescription("The maximum number of active streams on any channel over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    maxActiveStreams.removeTimeSeries(labelValues);
    maxActiveStreams.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMaxActiveStreams);

    final DerivedLongGauge minTotalActiveStreams =
        registry.addDerivedLongGauge(
            prefix + "min_total_active_streams",
            MetricOptions.builder()
                .setDescription("The minimum total number of active streams across all channels over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    minTotalActiveStreams.removeTimeSeries(labelValues);
    minTotalActiveStreams.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMinTotalActiveStreams);

    final DerivedLongGauge maxTotalActiveStreams =
        registry.addDerivedLongGauge(
            prefix + "max_total_active_streams",
            MetricOptions.builder()
                .setDescription("The maximum total number of active streams across all channels over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    maxTotalActiveStreams.removeTimeSeries(labelValues);
    maxTotalActiveStreams.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMaxTotalActiveStreams);

    final DerivedLongGauge minSessions =
        registry.addDerivedLongGauge(
            prefix + "min_sessions_per_channel",
            MetricOptions.builder()
                .setDescription("The minimum number of sessions on any channel over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    minSessions.removeTimeSeries(labelValues);
    minSessions.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMinSessions);

    final DerivedLongGauge maxSessions =
        registry.addDerivedLongGauge(
            prefix + "max_sessions_per_channel",
            MetricOptions.builder()
                .setDescription("The maximum number of sessions on any channel over the reporting period.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    maxSessions.removeTimeSeries(labelValues);
    maxSessions.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportMaxSessions);

    final DerivedLongCumulative numSessionsCreated =
        registry.addDerivedLongCumulative(
            prefix + "num_sessions_created",
            MetricOptions.builder()
                .setDescription("The number of sessions created since the creation of the pool.")
                .setLabelKeys(labelKeys)
                .setUnit(COUNT)
                .build());

    numSessionsCreated.removeTimeSeries(labelValues);
    numSessionsCreated.createTimeSeries(labelValues, this, ChannelPoolMonitor::reportNumSessionsCreated);

    final DerivedLongGauge minCompleted =
        registry.addDerivedLongGauge(
            prefix + "min_calls_per_channel",
            MetricOptions.builder()
                .setDescription("The minimum number of completed calls on any channel over the reporting period.")
                .setLabelKeys(labelKeysWithResult)
                .setUnit(COUNT)
                .build());

    minCompleted.removeTimeSeries(labelValuesSuccess);
    minCompleted.createTimeSeries(labelValuesSuccess, this, ChannelPoolMonitor::reportMinOkCalls);
    minCompleted.removeTimeSeries(labelValuesError);
    minCompleted.createTimeSeries(labelValuesError, this, ChannelPoolMonitor::reportMinErrCalls);

    final DerivedLongGauge maxCompleted =
        registry.addDerivedLongGauge(
            prefix + "max_calls_per_channel",
            MetricOptions.builder()
                .setDescription("The maximum number of completed calls on any channel over the reporting period.")
                .setLabelKeys(labelKeysWithResult)
                .setUnit(COUNT)
                .build());

    maxCompleted.removeTimeSeries(labelValuesSuccess);
    maxCompleted.createTimeSeries(labelValuesSuccess, this, ChannelPoolMonitor::reportMaxOkCalls);
    maxCompleted.removeTimeSeries(labelValuesError);
    maxCompleted.createTimeSeries(labelValuesError, this, ChannelPoolMonitor::reportMaxErrCalls);

    final DerivedLongCumulative numCalls =
        registry.addDerivedLongCumulative(
            prefix + "num_calls_completed",
            MetricOptions.builder()
                .setDescription("The number of calls completed across all channels.")
                .setLabelKeys(labelKeysWithResult)
                .setUnit(COUNT)
                .build());

    numCalls.removeTimeSeries(labelValuesSuccess);
    numCalls.createTimeSeries(labelValuesSuccess, this, ChannelPoolMonitor::reportTotalOkCalls);
    numCalls.removeTimeSeries(labelValuesError);
    numCalls.createTimeSeries(labelValuesError, this, ChannelPoolMonitor::reportTotalErrCalls);
  }

  private int reportMaxChannels() {
    return channelCounter.get();
  }

  private int reportMaxAllowedChannels() {
    return maxAllowedChannels;
  }

  public int reportMinReadyChannels() {
    int value = minReadyChannels;
    minReadyChannels = readyChannels.get();
    return value;
  }

  public int reportMaxReadyChannels() {
    int value = maxReadyChannels;
    maxReadyChannels = readyChannels.get();
    return value;
  }

  public long reportMinReadinessTime() {
    long value = minReadinessTime;
    minReadinessTime = 0;
    return value;
  }

  public long reportAvgReadinessTime() {
    long value = avgReadinessTime;
    avgReadinessTime = 0;
    readinessTimeOccurrences.set(0);
    totalReadinessTime.set(0);
    return value;
  }

  public long reportMaxReadinessTime() {
    long value = maxReadinessTime;
    maxReadinessTime = 0;
    return value;
  }

  public long reportNumChannelConnect() {
    return numChannelConnect.get();
  }

  public long reportNumChannelDisconnect() {
    return numChannelDisconnect.get();
  }

  public int reportMinActiveStreams() {
    int value = minActiveStreams;
    minActiveStreams = activeStreams.stream().mapToInt(AtomicInteger::get).min().orElse(0);
    return value;
  }

  public int reportMaxActiveStreams() {
    int value = maxActiveStreams;
    maxActiveStreams = activeStreams.stream().mapToInt(AtomicInteger::get).max().orElse(0);
    return value;
  }

  public int reportMinTotalActiveStreams() {
    int value = minTotalActiveStreams;
    minTotalActiveStreams = totalActiveStreams.get();
    return value;
  }

  public int reportMaxTotalActiveStreams() {
    int value = maxTotalActiveStreams;
    maxTotalActiveStreams = totalActiveStreams.get();
    return value;
  }

  public int reportMinSessions() {
    int value = minSessions;
    minSessions = sessions.stream().mapToInt(AtomicInteger::get).min().orElse(0);
    return value;
  }

  public int reportMaxSessions() {
    int value = maxSessions;
    maxSessions = sessions.stream().mapToInt(AtomicInteger::get).max().orElse(0);
    return value;
  }

  public int reportNumSessionsCreated() {
    return sessionsCreated.get();
  }

  public synchronized int reportMinOkCalls() {
    minOkReported = true;
    calcMinMaxOkCalls();
    return minOkCalls;
  }

  public synchronized int reportMaxOkCalls() {
    maxOkReported = true;
    calcMinMaxOkCalls();
    return maxOkCalls;
  }
  
  public long reportTotalOkCalls() {
    return totalOkCalls.get();
  }

  private void calcMinMaxOkCalls() {
    minOkCalls = okCalls.stream().mapToInt(AtomicInteger::get).min().orElse(0);
    maxOkCalls = okCalls.stream().mapToInt(AtomicInteger::get).max().orElse(0);
    if (minOkReported && maxOkReported) {
      minOkReported = false;
      maxOkReported = false;
      for (AtomicInteger okPerChannel : okCalls) {
        okPerChannel.set(0);
      }
    }
  }

  public synchronized int reportMinErrCalls() {
    minErrReported = true;
    calcMinMaxErrCalls();
    return minErrCalls;
  }

  public synchronized int reportMaxErrCalls() {
    maxErrReported = true;
    calcMinMaxErrCalls();
    return maxErrCalls;
  }

  public long reportTotalErrCalls() {
    return totalErrCalls.get();
  }

  private void calcMinMaxErrCalls() {
    minErrCalls = errCalls.stream().mapToInt(AtomicInteger::get).min().orElse(0);
    maxErrCalls = errCalls.stream().mapToInt(AtomicInteger::get).max().orElse(0);
    if (minErrReported && maxErrReported) {
      minErrReported = false;
      maxErrReported = false;
      for (AtomicInteger errPerChannel : errCalls) {
        errPerChannel.set(0);
      }
    }
  }

  public int incChannels() {
    notReadySince.add(0L);
    activeStreams.add(new AtomicInteger());
    sessions.add(new AtomicInteger());
    okCalls.add(new AtomicInteger());
    errCalls.add(new AtomicInteger());
    return channelCounter.getAndIncrement();
  }

  private void incReadyChannels() {
    numChannelConnect.incrementAndGet();
    final int newReady = readyChannels.incrementAndGet();
    if (maxReadyChannels < newReady) {
      maxReadyChannels = newReady;
    }
  }

  private void decReadyChannels() {
    numChannelDisconnect.incrementAndGet();
    final int newReady = readyChannels.decrementAndGet();
    if (minReadyChannels > newReady) {
      minReadyChannels = newReady;
    }
  }

  private void saveReadinessTime(int channelIdx) {
    if (notReadySince.get(channelIdx) == 0) {
      return;
    }
    long readinessTimeUs = (System.nanoTime() - notReadySince.get(channelIdx)) / 1000;
    if (minReadinessTime == 0 || readinessTimeUs < minReadinessTime) {
      minReadinessTime = readinessTimeUs;
    }
    if (readinessTimeUs > maxReadinessTime) {
      maxReadinessTime = readinessTimeUs;
    }
    long total = totalReadinessTime.addAndGet(readinessTimeUs);
    long occ = readinessTimeOccurrences.incrementAndGet();
    if (occ != 0) {
      avgReadinessTime = total / occ;
    }
  }

  public void channelStateUpdate(int channelIdx, ConnectivityState currentState, ConnectivityState newState) {
    if (newState == ConnectivityState.READY && currentState != ConnectivityState.READY) {
      saveReadinessTime(channelIdx);
      incReadyChannels();
    }
    if (newState == ConnectivityState.CONNECTING && currentState != ConnectivityState.CONNECTING) {
      notReadySince.set(channelIdx, System.nanoTime());
    }
    if (newState != ConnectivityState.READY && currentState == ConnectivityState.READY) {
      decReadyChannels();
    }
  }

  public void incActiveStreams(Integer channelIndex) {
    int totalActStreams = totalActiveStreams.incrementAndGet();
    if (maxTotalActiveStreams < totalActStreams) {
      maxTotalActiveStreams = totalActStreams;
    }
    if (channelIndex == null) {
      return;
    }
    int actStreams = activeStreams.get(channelIndex).incrementAndGet();
    if (maxActiveStreams < actStreams) {
      maxActiveStreams = actStreams;
    }
  }

  public void decActiveStreams(Integer channelIndex, Status status) {
    int totalActStreams = totalActiveStreams.decrementAndGet();
    if (minTotalActiveStreams > totalActStreams) {
      minTotalActiveStreams = totalActStreams;
    }
    boolean result = status.isOk();
    if (channelIndex == null) {
      return;
    }
    int actStreams = activeStreams.get(channelIndex).decrementAndGet();
    if (minActiveStreams > actStreams) {
      minActiveStreams = actStreams;
    }
    if (result) {
      okCalls.get(channelIndex).incrementAndGet();
      totalOkCalls.incrementAndGet();
    } else {
      errCalls.get(channelIndex).incrementAndGet();
      totalErrCalls.incrementAndGet();
    }
  }

  public void incSessions(Integer channelIndex, int delta) {
    sessionsCreated.addAndGet(delta);
    if (channelIndex == null) {
      return;
    }
    int count = sessions.get(channelIndex).addAndGet(delta);
    if (maxSessions < count) {
      maxSessions = count;
    }
  }

  public void decSessions(Integer channelIndex, int delta) {
    if (channelIndex == null) {
      return;
    }
    int count = sessions.get(channelIndex).addAndGet(-delta);
    if (minSessions > count) {
      minSessions = count;
    }
  }
}
