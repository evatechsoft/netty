/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.util;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.util.internal.StringUtil.NEWLINE;

public final class ResourceLeakDetector<T> {

    private static boolean disabled;

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ResourceLeakDetector.class);

    static {
        final boolean DISABLED = SystemPropertyUtil.getBoolean("io.netty.noResourceLeakDetection", false);
        logger.debug("-Dio.netty.noResourceLeakDetection: {}", DISABLED);
        disabled = DISABLED;
    }

    private static final int DEFAULT_SAMPLING_INTERVAL = 113;

    /**
     * Enables or disabled the resource leak detection.
     */
    public static void setEnabled(boolean enabled) {
        disabled = !enabled;
    }

    /**
     * Returns {@code true} if resource leak detection is enabled.
     */
    public static boolean isEnabled() {
        return !disabled;
    }

    /** the linked list of active resources */
    private final DefaultResourceLeak head = new DefaultResourceLeak(null);
    private final DefaultResourceLeak tail = new DefaultResourceLeak(null);

    private final ReferenceQueue<Object> refQueue = new ReferenceQueue<Object>();
    private final ConcurrentMap<String, Boolean> reportedLeaks = PlatformDependent.newConcurrentHashMap();

    private final String resourceType;
    private final int samplingInterval;
    private final long maxActive;
    private long active;
    private final AtomicBoolean loggedTooManyActive = new AtomicBoolean();

    private long leakCheckCnt;

    public ResourceLeakDetector(Class<?> resourceType) {
        this(StringUtil.simpleClassName(resourceType));
    }

    public ResourceLeakDetector(String resourceType) {
        this(resourceType, DEFAULT_SAMPLING_INTERVAL, Long.MAX_VALUE);
    }

    public ResourceLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        this(StringUtil.simpleClassName(resourceType), samplingInterval, maxActive);
    }

    public ResourceLeakDetector(String resourceType, int samplingInterval, long maxActive) {
        if (resourceType == null) {
            throw new NullPointerException("resourceType");
        }
        if (samplingInterval <= 0) {
            throw new IllegalArgumentException("samplingInterval: " + samplingInterval + " (expected: 1+)");
        }
        if (maxActive <= 0) {
            throw new IllegalArgumentException("maxActive: " + maxActive + " (expected: 1+)");
        }

        this.resourceType = resourceType;
        this.samplingInterval = samplingInterval;
        this.maxActive = maxActive;

        head.next = tail;
        tail.prev = head;
    }

    /**
     * Creates a new {@link ResourceLeak} which is expected to be closed via {@link ResourceLeak#close()} when the
     * related resource is deallocated.
     *
     * @return the {@link ResourceLeak} or {@code null}
     */
    public ResourceLeak open(T obj) {
        if (disabled || leakCheckCnt ++ % samplingInterval != 0) {
            return null;
        }

        reportLeak();

        return new DefaultResourceLeak(obj);
    }

    private void reportLeak() {
        if (!logger.isWarnEnabled()) {
            for (;;) {
                @SuppressWarnings("unchecked")
                DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
                if (ref == null) {
                    break;
                }
                ref.close();
            }
            return;
        }

        // Report too many instances.
        if (active * samplingInterval > maxActive && loggedTooManyActive.compareAndSet(false, true)) {
            logger.warn(
                    "LEAK: You are creating too many " + resourceType + " instances.  " +
                    resourceType + " is a shared resource that must be reused across the JVM," +
                    "so that only a few instances are created.");
        }

        // Detect and report previous leaks.
        for (;;) {
            @SuppressWarnings("unchecked")
            DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
            if (ref == null) {
                break;
            }

            ref.clear();

            if (!ref.close()) {
                continue;
            }

            String records = ref.toString();
            if (reportedLeaks.putIfAbsent(records, Boolean.TRUE) == null) {
                logger.warn(
                        "LEAK: {}.release() was not called before it is garbage-collected.{}", resourceType, records);
            }
        }
    }

    private final class DefaultResourceLeak extends PhantomReference<Object> implements ResourceLeak {

        private static final int MAX_RECORDS = 4;

        private String creationRecord;
        private final Deque<String> lastRecords = new ArrayDeque<String>();
        private final AtomicBoolean freed;
        private DefaultResourceLeak prev;
        private DefaultResourceLeak next;

        public DefaultResourceLeak(Object referent) {
            super(referent, referent != null? refQueue : null);

            if (referent != null) {
                creationRecord = newRecord();

                // TODO: Use CAS to update the list.
                synchronized (head) {
                    prev = head;
                    next = head.next;
                    head.next.prev = this;
                    head.next = this;
                    active ++;
                }
                freed = new AtomicBoolean();
            } else {
                freed = new AtomicBoolean(true);
            }
        }

        private String newRecord() {
            StringBuilder buf = new StringBuilder(4096);
            StackTraceElement[] array = new Throwable().getStackTrace();
            int recordsToSkip = 3;
            for (StackTraceElement e: array) {
                if (recordsToSkip > 0) {
                    recordsToSkip --;
                } else {
                    buf.append('\t');
                    buf.append(e.toString());
                    buf.append(NEWLINE);
                }
            }

            return buf.toString();
        }

        @Override
        public void record() {
            String value = newRecord();

            synchronized (lastRecords) {
                int size = lastRecords.size();
                if (size == 0 || !lastRecords.getLast().equals(value)) {
                    lastRecords.add(value);
                }
                if (size > MAX_RECORDS) {
                    lastRecords.removeFirst();
                }
            }
        }

        @Override
        public boolean close() {
            if (freed.compareAndSet(false, true)) {
                synchronized (head) {
                    active --;
                    prev.next = next;
                    next.prev = prev;
                    prev = null;
                    next = null;
                }
                return true;
            }
            return false;
        }

        public String toString() {
            StringBuilder buf = new StringBuilder(16384);
            int lastRecordCount = lastRecords.size();

            buf.append(NEWLINE);
            buf.append("Recent access records: ");
            buf.append(lastRecordCount);
            buf.append(NEWLINE);

            if (lastRecordCount > 0) {
                String[] lastRecords = this.lastRecords.toArray(new String[lastRecordCount]);
                for (int i = lastRecords.length - 1; i >= 0; i --) {
                    buf.append('#');
                    buf.append(i + 1);
                    buf.append(':');
                    buf.append(NEWLINE);
                    buf.append(lastRecords[i]);
                }
            }

            buf.append("Created at:");
            buf.append(NEWLINE);
            buf.append(creationRecord);
            buf.setLength(buf.length() - NEWLINE.length());

            return buf.toString();
        }
    }
}
