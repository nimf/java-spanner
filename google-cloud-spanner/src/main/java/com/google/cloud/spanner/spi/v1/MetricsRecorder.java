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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.grpc.ConnectivityState;
import io.opencensus.common.Scope;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagContextBuilder;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MetricsRecorder {
  private static final Logger logger = Logger.getLogger(MetricsRecorder.class.getName());
  private static MetricsRecorder instance = null;
  private static final StatsRecorder STATS_RECORDER = Stats.getStatsRecorder();
  private static final AtomicLong INFLIGHT_REQUESTS = new AtomicLong();
  private static final Map<String, AtomicLong> CHANNEL_SESSIONS = new HashMap<>();
  private static final Map<String, AtomicLong> CHANNEL_REQUESTS = new HashMap<>();
  private static final Tagger tagger = Tags.getTagger();
  public static final TagKey KEY_CHANNEL_NUM = TagKey.create("channel_num");
  public static final TagKey KEY_CHANNEL_STATE = TagKey.create("channel_state");
  public static final TagKey KEY_CHANNEL_INDEX = TagKey.create("channel_index");
  public static final TagKey KEY_METHOD = TagKey.create("method_name");
  public static final TagKey KEY_RPC_STATUS = TagKey.create("rpc_status");
  public static final MeasureLong READINESS_MS =
      MeasureLong.create(
          "tmp/channel_readiness_latency_ms",
          "The channel readiness latency in milliseconds.",
          "ms");
  public static final MeasureLong CHANNEL_STATE_ARRIVALS =
      MeasureLong.create(
          "tmp/num_channel_state_arrivals",
          "The number of channels switching to a state.",
          "channel");
  public static final MeasureLong CHANNELS_COUNT =
      MeasureLong.create(
          "tmp/num_channels",
          "The number of channels in a state.",
          "channel");
  public static final MeasureLong SESSIONS_COUNT =
      MeasureLong.create(
          "tmp/num_channel_sessions",
          "The number of Spanner sessions in a channel.",
          "session");
  public static final MeasureLong MAX_INFLIGHT_REQUESTS =
      MeasureLong.create(
          "tmp/max_in_flight_requests",
          "The max number of in-flight Spanner requests in a channel per minute.",
          "request");
  public static final MeasureLong SEND_DELAY_MS =
      MeasureLong.create(
          "tmp/send_delay_ms",
          "The request send delay in milliseconds.",
          "ms");
  public static final MeasureLong RPC_LATENCY_MS =
      MeasureLong.create(
          "tmp/rpc_latency_ms",
          "The RPC latency in milliseconds.",
          "ms");
  public static final MeasureLong COMPLETED_REQUESTS_COUNT =
      MeasureLong.create(
          "tmp/num_completed_requests",
          "The number of completed Spanner requests.",
          "request");

  private static final BucketBoundaries READINESS_BOUNDARIES =
      BucketBoundaries.create(Lists.newArrayList(0d, 10d, 50d, 100d, 200d, 500d, 1000d, 2000d, 5000d, 10000d));
  private static final BucketBoundaries SEND_DELAY_BOUNDARIES =
      BucketBoundaries.create(Lists.newArrayList(0d, 5d, 10d, 20d, 50d, 100d, 200d, 500d, 1000d, 2000d));
  private static final BucketBoundaries RPC_LATENCY_BOUNDARIES =
      BucketBoundaries.create(Lists.newArrayList(0d, 10d, 50d, 100d, 200d, 500d, 1000d, 2000d, 5000d, 10000d));

  private MetricsRecorder() {
    // Final views pattern should be
    // cloud.google.com/java/spanner/<name>
    // or
    // cloud.google.com/java/spanner/grpc/<name>

    // Register the view. It is imperative that this step exists,
    // otherwise recorded metrics will be dropped and never exported.
    View readinessView =
        View.create(
            Name.create("tmp/dist_channel_readiness_latency"),
            "The distribution of the channel readiness latencies.",
            READINESS_MS,
            Aggregation.Distribution.create(READINESS_BOUNDARIES),
            ImmutableList.of(KEY_CHANNEL_NUM));

    View stateArrivalsView =
        View.create(
            Name.create("tmp/num_channel_state_arrivals"),
            "The number of channels switching to a state.",
            CHANNEL_STATE_ARRIVALS,
            Aggregation.Count.create(),
            ImmutableList.of(KEY_CHANNEL_NUM, KEY_CHANNEL_STATE));

    View channelsCountView =
        View.create(
            Name.create("tmp/num_channels"),
            "The number of channels in a state.",
            CHANNELS_COUNT,
            Aggregation.LastValue.create(),
            ImmutableList.of(KEY_CHANNEL_NUM, KEY_CHANNEL_STATE));

    View sessionsCountView =
        View.create(
            Name.create("tmp/num_channel_sessions"),
            "The number of Spanner sessions in a channel.",
            SESSIONS_COUNT,
            Aggregation.LastValue.create(),
            ImmutableList.of(KEY_CHANNEL_INDEX));

    View requestsCountView =
        View.create(
            Name.create("tmp/max_in_flight_requests"),
            "The max number of in-flight Spanner requests in a channel per minute.",
            MAX_INFLIGHT_REQUESTS,
            Aggregation.LastValue.create(),
            ImmutableList.of(KEY_CHANNEL_INDEX, KEY_METHOD));

    View sendLatencyView =
        View.create(
            Name.create("tmp/dist_send_delays"),
            "The distribution of the send request delays.",
            SEND_DELAY_MS,
            Aggregation.Distribution.create(SEND_DELAY_BOUNDARIES),
            ImmutableList.of(KEY_CHANNEL_INDEX, KEY_METHOD));

    View rpcLatencyView =
        View.create(
            Name.create("tmp/dist_rpc_latency"),
            "The distribution of the RPC latency.",
            RPC_LATENCY_MS,
            Aggregation.Distribution.create(RPC_LATENCY_BOUNDARIES),
            ImmutableList.of(KEY_CHANNEL_INDEX, KEY_METHOD, KEY_RPC_STATUS));

    View completedRequestsCountView =
        View.create(
            Name.create("tmp/num_completed_requests"),
            "The number of completed Spanner requests in a channel.",
            COMPLETED_REQUESTS_COUNT,
            Aggregation.Count.create(),
            ImmutableList.of(KEY_CHANNEL_INDEX, KEY_METHOD, KEY_RPC_STATUS));

    ViewManager viewManager = Stats.getViewManager();

    viewManager.registerView(readinessView);
    viewManager.registerView(stateArrivalsView);
    viewManager.registerView(channelsCountView);
    viewManager.registerView(sessionsCountView);
    viewManager.registerView(requestsCountView);
    viewManager.registerView(sendLatencyView);
    viewManager.registerView(rpcLatencyView);
    viewManager.registerView(completedRequestsCountView);
    try {
      // Enable OpenCensus exporters to export metrics to Stackdriver Monitoring.
      // Exporters use Application Default Credentials to authenticate.

      // See https://developers.google.com/identity/protocols/application-default-credentials
      // for more details.
      StackdriverStatsExporter.createAndRegister();
      logger.log(Level.INFO, "StackDriver exporter registered successfully.");
    } catch (Throwable e) {
      logger.log(Level.WARNING, "Cannot register StackDriver exporter.", e);
    }
  }

  private static MetricsRecorder getMetricRecorder() {
    if (instance != null) {
      return instance;
    }
    synchronized (MetricsRecorder.class) {
      if (instance != null) {
        return instance;
      }
      instance = new MetricsRecorder();
    }
    return instance;
  }

  private void recordMetric(TagKey tagKey, String tagValue, MeasureLong ml, long value) {
    TagContext tagCtx = tagger.emptyBuilder()
        .putPropagating(tagKey, TagValue.create(tagValue))
        .build();
    try (Scope ignored = tagger.withTagContext(tagCtx)) {
      STATS_RECORDER.newMeasureMap().put(ml, value).record();
    }
  }

  private void recordMetric(TagKey[] tagKeys, String[] tagValues, MeasureLong ml, long value) {
    TagContextBuilder builder = tagger.emptyBuilder();
    for (int i = 0; i < Math.min(tagKeys.length, tagValues.length); i++) {
      builder.putPropagating(tagKeys[i], TagValue.create(tagValues[i]));
    }
    TagContext tagCtx = builder.build();
    try (Scope ignored = tagger.withTagContext(tagCtx)) {
      STATS_RECORDER.newMeasureMap().put(ml, value).record();
    }
  }

  public static void recordChannelReadinessLatency(long latency, String channelNum) {
    getMetricRecorder().recordMetric(
        KEY_CHANNEL_NUM,
        channelNum,
        READINESS_MS,
        latency
    );
  }

  public static void recordChannelStateTransition(String channelNum, ConnectivityState fromState, ConnectivityState toState) {
    final TagKey[] tagKeys = {KEY_CHANNEL_NUM, KEY_CHANNEL_STATE};
    getMetricRecorder().recordMetric(
        tagKeys,
        new String[]{channelNum, toState.name()},
        CHANNEL_STATE_ARRIVALS,
        1
    );

    getMetricRecorder().recordMetric(
        tagKeys,
        new String[]{channelNum, toState.name()},
        CHANNELS_COUNT,
        1
    );

    if (fromState != null) {
      getMetricRecorder().recordMetric(
          tagKeys,
          new String[]{channelNum, fromState.name()},
          CHANNELS_COUNT,
          0
      );
    }
  }

  public static void recordSessionsCount(long sessions, String channelIndex) {
    if (channelIndex == null) {
      channelIndex = "null";
    }
    AtomicLong counter = CHANNEL_SESSIONS.get(channelIndex);
    if (counter == null) {
      counter = new AtomicLong();
      CHANNEL_SESSIONS.put(channelIndex, counter);
    }
    getMetricRecorder().recordMetric(
        KEY_CHANNEL_INDEX,
        channelIndex,
        SESSIONS_COUNT,
        counter.addAndGet(sessions)
    );
  }

  public static void reportRequestStart(String channelIndex, String method) {
    // if (channelIndex == null) {
    //   channelIndex = "null";
    // }
    // if (method == null) {
    //   method = "null";
    // }
    // String key = channelIndex + ":" + method;
    // AtomicLong counter = CHANNEL_REQUESTS.get(key);
    // if (counter == null) {
    //   counter = new AtomicLong();
    //   // logger.log(Level.INFO ,"NEW COUNTER ON INCR FOR {0}", key);
    //   CHANNEL_REQUESTS.put(key, counter);
    // }
    // final TagKey[] tagKeys = {KEY_CHANNEL_INDEX, KEY_METHOD};
    // final String[] tagValues = {channelIndex, method};
    //
    // long val = counter.incrementAndGet();
    // // logger.log(Level.INFO , "INCR {0} REQUESTS", key);
    // // logger.log(Level.INFO ,"INCR REQUESTS TO {0}", val);
    // getMetricRecorder().recordMetric(
    //     tagKeys,
    //     tagValues,
    //     MAX_INFLIGHT_REQUESTS,
    //     counter.incrementAndGet()
    // );
  }

  public static void reportRequestSend(long sendDelay, String channelIndex, String method) {
    if (channelIndex == null) {
      channelIndex = "null";
    }
    if (method == null) {
      method = "null";
    }

    final TagKey[] tagKeys = {KEY_CHANNEL_INDEX, KEY_METHOD};
    final String[] tagValues = {channelIndex, method};

    getMetricRecorder().recordMetric(
        tagKeys,
        tagValues,
        SEND_DELAY_MS,
        sendDelay
    );
  }

  public static void reportRequestEnd(long latency, String status, String channelIndex, String method) {
    if (status == null) {
      status = "null";
    }
    if (channelIndex == null) {
      channelIndex = "null";
    }
    if (method == null) {
      method = "null";
    }
    // String key = channelIndex + ":" + method;
    // AtomicLong counter = CHANNEL_REQUESTS.get(key);
    // if (counter == null) {
    //   counter = new AtomicLong();
    //   // logger.log(Level.INFO ,"NEW COUNTER ON DECR FOR {0}", key);
    //   CHANNEL_REQUESTS.put(key, counter);
    // }
    //
    // long val = counter.decrementAndGet();
    // // logger.log(Level.INFO , "DECR {0} REQUESTS", key);
    // // logger.log(Level.INFO , "DECR REQUESTS TO {0}", val);
    // getMetricRecorder().recordMetric(
    //     new TagKey[]{KEY_CHANNEL_INDEX, KEY_METHOD},
    //     new String[]{channelIndex, method},
    //     MAX_INFLIGHT_REQUESTS,
    //     val
    // );

    getMetricRecorder().recordMetric(
        new TagKey[]{KEY_CHANNEL_INDEX, KEY_METHOD, KEY_RPC_STATUS},
        new String[]{channelIndex, method, status},
        RPC_LATENCY_MS,
        latency
    );

    getMetricRecorder().recordMetric(
        new TagKey[]{KEY_CHANNEL_INDEX, KEY_METHOD, KEY_RPC_STATUS},
        new String[]{channelIndex, method, status},
        COMPLETED_REQUESTS_COUNT,
        1
    );
  }
}
