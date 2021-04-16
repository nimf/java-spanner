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

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.CompressorRegistry;
import io.grpc.ConnectivityState;
import io.grpc.DecompressorRegistry;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.NameResolver;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.annotation.Nullable;

class MonitoredChannelBuilder extends ManagedChannelBuilder {
  private final ManagedChannelBuilder delegate;
  private final int numChannels;
  private final AtomicInteger monitoredChannelCounter;
  private static final Logger logger = Logger.getLogger(MonitoredChannelBuilder.class.getName());
  static final CallOptions.Key<Integer> AFFINITY_CALL_OPTION_KEY =
      CallOptions.Key.createWithDefault("affinity", null);
  private enum SessionOp{CREATE, USE, DELETE}
  private static final HashMap<String, String> SESSION_KEYS = new HashMap<String, String>(){{
    put("google.spanner.v1.Spanner/CreateSession", "name");
    put("google.spanner.v1.Spanner/GetSession", "name");
    put("google.spanner.v1.Spanner/DeleteSession", "name");
    put("google.spanner.v1.Spanner/BatchCreateSessions", "session.name");
    put("google.spanner.v1.Spanner/ExecuteSql", "session");
    put("google.spanner.v1.Spanner/ExecuteStreamingSql", "session");
    put("google.spanner.v1.Spanner/Read", "session");
    put("google.spanner.v1.Spanner/StreamingRead", "session");
    put("google.spanner.v1.Spanner/BeginTransaction", "session");
    put("google.spanner.v1.Spanner/Commit", "session");
    put("google.spanner.v1.Spanner/PartitionRead", "session");
    put("google.spanner.v1.Spanner/PartitionQuery", "session");
    put("google.spanner.v1.Spanner/Rollback", "session");
  }};
  private static final HashMap<String, SessionOp> SESSION_OPS = new HashMap<String, SessionOp>(){{
    put("google.spanner.v1.Spanner/CreateSession", SessionOp.CREATE);
    put("google.spanner.v1.Spanner/BatchCreateSessions", SessionOp.CREATE);
    put("google.spanner.v1.Spanner/DeleteSession", SessionOp.DELETE);
  }};

  private static Integer getChannelIndex(int poolSize, @Nullable Integer affinity) {
    if (affinity == null) {
      return null;
    }
    int index = affinity % poolSize;
    index = Math.abs(index);
    // If index is the most negative int, abs(index) is still negative.
    if (index < 0) {
      index = 0;
    }
    return index;
  }

  /**
   * Get the affinity key from the request message.
   *
   * <p>The message can be written in the format of:
   *
   * <p>session1: "the-key-we-want" \n transaction_id: "not-useful" \n transaction { \n session2:
   * "another session"} \n}
   *
   * <p>If the (affinity) name is "session1", it will return "the-key-we-want".
   *
   * <p>If you want to get the key "another session" in the nested message, the name should be
   * "session1.session2".
   */
  private static List<String> getKeysFromMessage(MessageOrBuilder msg, String name) {
    // The field names in a nested message name are split by '.'.
    int currentLength = name.indexOf('.');
    String currentName = name;
    if (currentLength != -1) {
      currentName = name.substring(0, currentLength);
    }

    List<String> keys = new ArrayList<>();
    Map<FieldDescriptor, Object> obs = msg.getAllFields();
    for (Map.Entry<FieldDescriptor, Object> entry : obs.entrySet()) {
      if (entry.getKey().getName().equals(currentName)) {
        if (currentLength == -1 && entry.getValue() instanceof String) {
          // Value of the current field.
          keys.add(entry.getValue().toString());
        } else if (currentLength != -1 && entry.getValue() instanceof MessageOrBuilder) {
          // One nested MessageOrBuilder.
          keys.addAll(
              getKeysFromMessage(
                  (MessageOrBuilder) entry.getValue(), name.substring(currentLength + 1)));
        } else if (currentLength != -1 && entry.getValue() instanceof List) {
          // Repeated nested MessageOrBuilder.
          List<?> list = (List<?>) entry.getValue();
          if (list.size() > 0 && list.get(0) instanceof MessageOrBuilder) {
            for (Object o : list) {
              keys.addAll(
                  getKeysFromMessage(
                      (MessageOrBuilder) o, name.substring(currentLength + 1)));
            }
          }
        }
      }
    }
    return keys;
  }

  MonitoredChannelBuilder(ManagedChannelBuilder builder, int numChannels, AtomicInteger monitoredChannelCounter) {
    delegate = builder;
    this.numChannels = numChannels;
    this.monitoredChannelCounter = monitoredChannelCounter;
  }

  class ChannelStateMonitor implements Runnable {
    final private ManagedChannel channel;
    final private String channelNum;
    private ConnectivityState currentState;
    private long notReadySince;

    private ChannelStateMonitor(ManagedChannel channel) {
      this.channel = channel;
      this.channelNum = String.valueOf(monitoredChannelCounter.getAndIncrement());
      notReadySince = System.currentTimeMillis();
      run();
    }

    @Override
    public void run() {
      ConnectivityState newState = channel.getState(true);
      MetricsRecorder.recordChannelStateTransition(channelNum, currentState, newState);
      if (newState == ConnectivityState.READY && currentState != ConnectivityState.READY) {
        long readinessTime = System.currentTimeMillis() - notReadySince;
        MetricsRecorder.recordChannelReadinessLatency(readinessTime, channelNum);
        logger.fine(String.format("Replace this with your metrics library call. Report %s ms channel readiness time. Channel number %s", readinessTime, channelNum));
      }
      if (newState != ConnectivityState.READY && currentState == ConnectivityState.READY) {
        notReadySince = System.currentTimeMillis();
      }
      if (currentState != null) {
        logger.fine(String.format("Replace this with your metrics library call. Report decremented channels count. State %s, channel number %s", currentState, channelNum));
      }
      currentState = newState;
      if (newState != ConnectivityState.SHUTDOWN) {
        logger.fine(String.format("Replace this with your metrics library call. Report incremented channels count. State %s, channel number %s", newState, channelNum));
        channel.notifyWhenStateChanged(newState, this);
      }
    }
  }

  class CallMonitor {
    private final String channelIndex;
    private final long startTime;
    private long sendTime;
    private long receiveTime;
    @Nullable private String methodName;
    @Nullable private SessionOp sessOp;
    private List<String> sessions = new ArrayList<>();
    private String session = null;

    CallMonitor(@Nullable Integer channelIndex, MethodDescriptor method) {
      startTime = System.currentTimeMillis();
      this.channelIndex = String.valueOf(channelIndex);
      this.methodName = method.getFullMethodName();
      MetricsRecorder.reportRequestStart(this.channelIndex, methodName);
    }

    public <RespT, ReqT> void onSend(MethodDescriptor<ReqT,RespT> method, ReqT message) {
      if (sendTime > 0) {
        return;
      }
      sendTime = System.currentTimeMillis();
      // Fetch sessions
      sessOp = SESSION_OPS.get(methodName);
      if (sessOp == null) {
        sessOp = SessionOp.USE;
      }
      final String sessionKey = SESSION_KEYS.get(methodName);
      if (sessOp != SessionOp.CREATE && sessionKey != null) {
        sessions.addAll(getKeysFromMessage((MessageOrBuilder) message, sessionKey));
      }
      // For a request within a session there must be only one session.
      if (sessOp != SessionOp.CREATE && sessions.size() == 1) {
        session = sessions.get(0);
      }
      MetricsRecorder.reportRequestSend(sendTime - startTime, channelIndex, methodName);
      logger.fine(String.format("Replace this with your metrics library call. Report incremented requests count. Channel index %s, method %s, session %s", channelIndex, methodName, session));
      logger.fine(String.format("Replace this with your metrics library call. Report %d ms send delay. Channel index %s, method %s, session %s", sendTime - startTime, channelIndex, methodName, session));
      if (sessOp == SessionOp.DELETE) {
        MetricsRecorder.recordSessionsCount(-1, channelIndex);
        logger.fine(String.format(
            "Replace this with your metrics library call. Report decremented session count. Channel index %s, session %s", channelIndex, session));
      }
    }

    public <ReqT, RespT> void onMessage(RespT message) {
      if (receiveTime > 0) {
        return;
      }
      receiveTime = System.currentTimeMillis();
      if (sessOp == SessionOp.CREATE) {
        // Fetch sessions
        final String sessionKey = SESSION_KEYS.get(methodName);
        if (sessionKey != null) {
          final List<String> sessions = getKeysFromMessage((MessageOrBuilder) message, sessionKey);
          if (sessions.size() > 0) {
            MetricsRecorder.recordSessionsCount(sessions.size(), channelIndex);
            // for (String session : sessions) {
            //   logger.fine(String.format(
            //       "Replace this with your metrics library call. Report incremented session count. Channel index %s, session %s",
            //       channelIndex, session));
            // }
          }
        }
      }
    }

    void onClose(Status status) {
      MetricsRecorder.reportRequestEnd(System.currentTimeMillis() - startTime, status.getCode().name(), channelIndex, methodName);
      logger.fine(String.format(
          "Replace this with your metrics library call. Report decremented requests count. Channel index %s, method %s, session %s, latency: %d",
          channelIndex, methodName, session, System.currentTimeMillis() - startTime));
    }
  }

  @Override
  public ManagedChannelBuilder directExecutor() {
    return delegate.directExecutor();
  }

  @Override
  public ManagedChannelBuilder executor(Executor executor) {
    return delegate.executor(executor);
  }

  @Override
  public ManagedChannelBuilder intercept(ClientInterceptor... clientInterceptors) {
    return delegate.intercept(clientInterceptors);
  }

  @Override
  public ManagedChannelBuilder userAgent(String s) {
    return delegate.userAgent(s);
  }

  @Override
  public ManagedChannelBuilder overrideAuthority(String s) {
    return delegate.overrideAuthority(s);
  }

  @Override
  public ManagedChannelBuilder nameResolverFactory(NameResolver.Factory factory) {
    return delegate.nameResolverFactory(factory);
  }

  @Override
  public ManagedChannelBuilder decompressorRegistry(DecompressorRegistry decompressorRegistry) {
    return delegate.decompressorRegistry(decompressorRegistry);
  }

  @Override
  public ManagedChannelBuilder compressorRegistry(CompressorRegistry compressorRegistry) {
    return delegate.compressorRegistry(compressorRegistry);
  }

  @Override
  public ManagedChannelBuilder idleTimeout(long l, TimeUnit timeUnit) {
    return delegate.idleTimeout(l, timeUnit);
  }

  @Override
  public ManagedChannel build() {
    delegate.intercept(new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          final MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        Integer affinity = callOptions.getOption(AFFINITY_CALL_OPTION_KEY);
        Integer channelIndex = getChannelIndex(numChannels, affinity);
        final CallMonitor monitor = new CallMonitor(channelIndex, method);
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)) {
          @Override
          public void start(Listener<RespT> responseListener, Metadata headers) {
            super.start(
                new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                    responseListener) {
                  @Override
                  public void onMessage(RespT message) {
                    monitor.onMessage(message);
                    super.onMessage(message);
                  }

                  @Override
                  public void onClose(Status status, Metadata trailers) {
                    monitor.onClose(status);
                    super.onClose(status, trailers);
                  }
                },
                headers);
          }

          @Override
          public void sendMessage(ReqT message) {
            monitor.onSend(method, message);
            super.sendMessage(message);
          }
        };
      }
    });
    ManagedChannel channel = delegate.build();
    new ChannelStateMonitor(channel);
    return channel;
  }

  @Override
  public ManagedChannelBuilder intercept(List list) {
    return delegate.intercept(list);
  }
}
