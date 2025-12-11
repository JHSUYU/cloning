package com.rits.cloning;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.*;

public class AutoPilotHandler {

    /**
     * 克隆类型枚举
     */
    public enum CloneType {
        REPLACE_OBJECT,  // 替换整个对象
        REPLACE_FIELD,   // 替换字段值
        DEFAULT          // 使用默认克隆行为
    }

    /**
     * 检查对象是否为 IndexReader 或其子类
     */
    public static boolean isIndexReader(Object o) {
        if (o == null) return false;

        Class<?> clazz = o.getClass();
        while (clazz != null) {
            if (clazz.getName().equals("org.apache.lucene.index.IndexReader")) {
                return true;
            }
            clazz = clazz.getSuperclass();
        }
        return false;
    }

    /**
     * 创建一个新的空的 parentReaders Set
     */
    private static Set<?> createNewParentReadersSet() {
        return Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<Object, Boolean>()));
    }

    /**
     * 注册所有需要跳过克隆的类
     */
    public static void registerDontCloneClasses(Cloner cloner) {
        // 文件系统相关类
        String[] fileSystemClasses = {
                "sun.nio.fs.UnixPath",
                "sun.nio.fs.WindowsPath",
                "sun.nio.fs.UnixFileSystem",
                "sun.nio.fs.WindowsFileSystem",
                "sun.nio.fs.UnixFileSystemProvider",
                "sun.nio.fs.WindowsFileSystemProvider",
                "java.nio.file.Path",
                "java.io.File",
                "java.io.RandomAccessFile",
                "java.nio.channels.FileChannel",
                "java.nio.channels.FileLock"
        };

        // Lucene Directory 相关类
        String[] luceneDirectoryClasses = {
                "org.apache.lucene.store.Directory",
                "org.apache.lucene.store.FSDirectory",
                "org.apache.lucene.store.MMapDirectory",
                "org.apache.lucene.store.NIOFSDirectory",
                "org.apache.lucene.store.SimpleFSDirectory",
                "org.apache.lucene.store.RAMDirectory",
                "org.apache.lucene.store.ByteBuffersDirectory",
                "org.apache.lucene.store.FilterDirectory",
                "org.apache.lucene.store.TrackingDirectoryWrapper",
                "org.apache.lucene.store.LockValidatingDirectoryWrapper",
                "org.apache.lucene.store.Lock",
                "org.apache.lucene.store.LockFactory",
                "org.apache.lucene.store.IndexInput",
                "org.apache.lucene.store.IndexOutput",
                "org.apache.lucene.store.IOContext"
        };

        // 锁相关类
        String[] lockingClasses = {
                "java.util.concurrent.locks.Lock",
                "java.util.concurrent.locks.ReentrantLock",
                "java.util.concurrent.locks.ReadWriteLock",
                "java.util.concurrent.locks.ReentrantReadWriteLock",
                "java.util.concurrent.Semaphore",
                "java.util.concurrent.CountDownLatch",
                "java.util.concurrent.CyclicBarrier",
                "java.util.concurrent.Phaser",
                "java.lang.Object",
                "org.apache.lucene.index.DocumentsWriterFlushQueue",
                "org.apache.lucene.index.DocumentsWriterDeleteQueue"
        };

        // 线程相关类
        String[] threadingClasses = {
                "java.lang.Thread",
                "java.lang.ThreadGroup",
                "java.util.concurrent.ThreadPoolExecutor",
                "java.util.concurrent.ExecutorService",
                "java.util.concurrent.Executor",
                "java.util.concurrent.ScheduledExecutorService",
                "java.util.concurrent.ForkJoinPool",
                "org.apache.lucene.index.MergeScheduler",
                "org.apache.lucene.index.ConcurrentMergeScheduler",
                "org.apache.lucene.index.SerialMergeScheduler"
        };

        // 配置相关类
        String[] configClasses = {
                "org.apache.lucene.util.InfoStream",
                "org.apache.lucene.index.IndexWriterConfig",
                "org.apache.lucene.index.LiveIndexWriterConfig",
                "org.apache.lucene.index.MergePolicy",
                "org.apache.lucene.index.IndexDeletionPolicy",
                "org.apache.lucene.index.FlushPolicy",
                "org.apache.lucene.index.FieldInfos$FieldNumbers",
                "org.apache.lucene.analysis.Analyzer",
                "org.apache.lucene.codecs.Codec",
                "org.apache.lucene.search.similarities.Similarity"
        };

        // 回调相关类
        String[] callbackClasses = {
                "org.apache.lucene.index.IndexWriter$Event",
                "org.apache.lucene.index.IndexWriter$EventQueue",
                "org.apache.lucene.index.IndexWriter$IndexReaderWarmer",
                "org.apache.lucene.index.SegmentInfos$FindSegmentsFile",
                "org.apache.lucene.index.IndexReader$CacheHelper",
                "org.apache.lucene.index.IndexReader$ClosedListener",
                "org.apache.lucene.index.QueryTimeout",
                "org.apache.lucene.index.DocumentsWriter$FlushNotifications"
        };

        // Metrics 相关类
        String[] metricsClasses = {
                "com.codahale.metrics.Timer",
                "com.codahale.metrics.Meter",
                "com.codahale.metrics.Counter",
                "com.codahale.metrics.Histogram",
                "com.codahale.metrics.Gauge",
                "com.codahale.metrics.MetricRegistry"
        };

        // 池化相关类
        String[] poolingClasses = {
                "org.apache.lucene.index.ReaderPool",
                "org.apache.lucene.index.ReadersAndUpdates",
                "org.apache.lucene.index.BufferedUpdatesStream",
                "org.apache.lucene.index.SegmentReader",
                "org.apache.lucene.util.Accountable",
                "org.apache.lucene.util.ByteBlockPool",
                "org.apache.lucene.util.RecyclingByteBlockAllocator"
        };

        // 原子类
        String[] atomicClasses = {
                "java.util.concurrent.ConcurrentLinkedQueue",
                "java.util.concurrent.ConcurrentHashMap"
        };

        // Solr 相关类
        String[] solrWorkAround = {
                "org.apache.solr.metrics.SolrMetricsContext"
        };

        // 测试相关类
        String[] testingClasses = {
                "com.carrotsearch.randomizedtesting.ThreadLeakControl",
                "com.carrotsearch.randomizedtesting.RandomizedRunner",
                "com.carrotsearch.randomizedtesting.rules.StatementAdapter",
                "org.apache.lucene.util.TestRule",
                "junit.framework.TestCase"
        };

        // 日志相关类
        String[] commonLogClasses = {
                "org.slf4j.Logger",
                "org.slf4j.LoggerFactory",
                "org.apache.logging.log4j.Logger",
                "org.apache.logging.log4j.LogManager",
                "java.util.logging.Logger"
        };

        // 其他类
        String[] otherClasses = {
                "java.lang.ClassLoader",
                "java.net.URLClassLoader",
                "java.net.URL",
                "java.util.regex.Pattern",
                "java.util.regex.Matcher",
                "java.io.PrintStream",
        };

        // 注册所有类
        for (String[] classArray : new String[][]{
                fileSystemClasses, luceneDirectoryClasses, lockingClasses,
                threadingClasses, configClasses, callbackClasses,
                poolingClasses, atomicClasses, testingClasses, metricsClasses,
                solrWorkAround, commonLogClasses, otherClasses}) {
            for (String className : classArray) {
                try {
                    Class<?> clazz = Class.forName(className);
                    cloner.dontClone(clazz);
                } catch (ClassNotFoundException e) {
                    // 类不存在，忽略
                }
            }
        }

        // 注册函数式接口
        cloner.dontCloneInstanceOf(Function.class);
        cloner.dontCloneInstanceOf(Runnable.class);
        cloner.dontCloneInstanceOf(Callable.class);
        cloner.dontCloneInstanceOf(Consumer.class);
        cloner.dontCloneInstanceOf(Supplier.class);
        cloner.dontCloneInstanceOf(Predicate.class);
    }

    /**
     * 检查字段是否应该跳过克隆（基于State类的CustomStrategy）
     */
    private static boolean shouldSkipFieldBasedOnStrategy(Object obj, Field field) {
        if (field == null || field.getType() == null) {
            return true;
        }

        if (obj != null && (obj.getClass().getName().contains("$Lambda") ||
                obj.getClass().isSynthetic())) {
            return true;
        }

        try {
            field.setAccessible(true);
            Object value = field.get(obj);
            if (value != null) {
                String valueClassName = value.getClass().getName();

                // 文件系统相关
                if (valueClassName.startsWith("sun.nio.fs.") ||
                        valueClassName.startsWith("java.nio.file.") ||
                        valueClassName.startsWith("org.apache.lucene.store.")) {
                    return true;
                }

                // Codecs 相关
                if(valueClassName.startsWith("org.apache.lucene.codecs")){
                    return true;
                }

                // Lambda 表达式
                if (valueClassName.contains("Lambda") ||
                        valueClassName.contains("$Lambda") ||
                        value.getClass().isSynthetic()) {
                    return true;
                }

                // Solr 相关（除了 SolrIndexWriter）
                if(valueClassName.contains("org.apache.solr") &&
                        (!valueClassName.contains("org.apache.solr.update.SolrIndexWriter"))){
                    return true;
                }

                // 匿名内部类
                if (value.getClass().isAnonymousClass()) {
                    return true;
                }

                // 线程相关
                if (value instanceof Thread || value instanceof ThreadGroup ||
                        value instanceof ExecutorService) {
                    return true;
                }
            }
        } catch (IllegalAccessException e) {
            // 忽略访问错误
        }

        return false;
    }

    /**
     * 深度克隆 IndexReader，但对 parentReaders 字段特殊处理
     */
    private static Object cloneIndexReader(Cloner cloner, Object originalObject, Map<Object, Object> clones) {
        try {
            Class<?> clazz = originalObject.getClass();

            // 使用 Cloner 的实例化策略创建新实例
            Object newInstance = ObjenesisInstantiationStrategy.getInstance().newInstance(clazz);

            // 将新实例加入到已克隆映射中，防止循环引用
            if (clones != null) {
                clones.put(originalObject, newInstance);
            }

            // 遍历所有字段进行克隆
            Class<?> currentClass = clazz;
            while (currentClass != null && currentClass != Object.class) {
                Field[] fields = currentClass.getDeclaredFields();
                for (Field field : fields) {
                    // 跳过静态字段
                    if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }

                    field.setAccessible(true);

                    // 检查是否是 parentReaders 字段
                    if ("parentReaders".equals(field.getName())) {
                        System.out.println("AUTOPILOTHandler: Found parentReaders field, replacing with new empty Set");
                        // 对 parentReaders 字段特殊处理：设置为新的空 Set
                        field.set(newInstance, createNewParentReadersSet());
                    } else if (shouldSkipFieldBasedOnStrategy(originalObject, field)) {
                        // 如果基于策略应该跳过，直接复制原值
                        Object fieldValue = field.get(originalObject);
                        field.set(newInstance, fieldValue);
                    } else {
                        // 其他字段正常深度克隆
                        Object fieldValue = field.get(originalObject);
                        if (fieldValue != null) {
                            // 直接使用 Cloner 的 cloneInternal 方法进行深度克隆
                            Object clonedFieldValue = cloner.cloneInternal(fieldValue, clones);
                            field.set(newInstance, clonedFieldValue);
                        } else {
                            field.set(newInstance, null);
                        }
                    }
                }
                currentClass = currentClass.getSuperclass();
            }

            System.out.println("AUTOPILOTHandler: Successfully cloned IndexReader with special parentReaders handling");
            return newInstance;

        } catch (Exception e) {
            System.err.println("AUTOPILOTHandler: Failed to clone IndexReader: " + e.getMessage());
            e.printStackTrace();
            // 如果克隆失败，返回null让Cloner使用默认克隆
            return null;
        }
    }

    /**
     * 处理结果类
     */
    public static class HandleResult {
        public final Object replacementObject;
        public final Object replacementFieldValue;
        public final CloneType cloneType;

        public HandleResult(Object replacementObject, Object replacementFieldValue, CloneType cloneType) {
            this.replacementObject = replacementObject;
            this.replacementFieldValue = replacementFieldValue;
            this.cloneType = cloneType;
        }

        public static HandleResult replaceObject(Object replacement) {
            return new HandleResult(replacement, null, CloneType.REPLACE_OBJECT);
        }

        public static HandleResult replaceField(Object fieldValue) {
            return new HandleResult(null, fieldValue, CloneType.REPLACE_FIELD);
        }

        public static HandleResult useDefault() {
            return new HandleResult(null, null, CloneType.DEFAULT);
        }
    }

    /**
     * 主要的自定义克隆方法
     *
     * @param cloner Cloner实例
     * @param originalObject 原始对象
     * @param clones 已克隆对象的映射表
     * @return 处理结果
     */
    public static HandleResult handleClone(Cloner cloner, Object originalObject, Map<Object, Object> clones) {

        // 检查是否为 IndexReader 或其子类
        if (isIndexReader(originalObject)) {
            System.out.println("AUTOPILOTHandler: Detected IndexReader or subclass: "
                    + originalObject.getClass().getName());

            // 执行特殊的克隆逻辑
            Object clonedIndexReader = cloneIndexReader(cloner, originalObject, clones);

            if (clonedIndexReader != null) {
                // 返回克隆好的对象，替换整个对象
                return HandleResult.replaceObject(clonedIndexReader);
            } else {
                System.err.println("AUTOPILOTHandler: Failed to clone IndexReader, falling back to default cloning");
                // 克隆失败，使用默认克隆
                return HandleResult.useDefault();
            }
        }

        // 不是 IndexReader，使用默认克隆行为
        return HandleResult.useDefault();
    }

    /**
     * 检查是否应该跳过某个字段的克隆
     * 由于我们在handleClone中已经处理了IndexReader，这个方法通常不会被调用
     *
     * @param object 包含字段的对象
     * @param field 要检查的字段
     * @return true 如果应该跳过克隆，false 否则
     */
    public static boolean shouldSkipField(Object object, Field field) {
        return false;
    }

    /**
     * 调试方法：打印对象信息
     */
    private static void debugPrint(String message) {
        if (System.getProperty("autopilot.debug") != null) {
            System.out.println("[AUTOPILOT DEBUG] " + message);
        }
    }
}