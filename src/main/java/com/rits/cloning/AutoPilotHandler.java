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

    
    public enum CloneType {
        REPLACE_OBJECT,  
        REPLACE_FIELD,   
        DEFAULT          
    }

    
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

    
    private static Set<?> createNewParentReadersSet() {
        return Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<Object, Boolean>()));
    }

    
    public static void registerDontCloneClasses(Cloner cloner) {
        
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

        
        String[] metricsClasses = {
                "com.codahale.metrics.Timer",
                "com.codahale.metrics.Meter",
                "com.codahale.metrics.Counter",
                "com.codahale.metrics.Histogram",
                "com.codahale.metrics.Gauge",
                "com.codahale.metrics.MetricRegistry"
        };

        
        String[] poolingClasses = {
                "org.apache.lucene.index.ReaderPool",
                "org.apache.lucene.index.ReadersAndUpdates",
                "org.apache.lucene.index.BufferedUpdatesStream",
                "org.apache.lucene.index.SegmentReader",
                "org.apache.lucene.util.Accountable",
                "org.apache.lucene.util.ByteBlockPool",
                "org.apache.lucene.util.RecyclingByteBlockAllocator"
        };

        
        String[] atomicClasses = {
                "java.util.concurrent.ConcurrentLinkedQueue",
                "java.util.concurrent.ConcurrentHashMap"
        };

        
        String[] solrWorkAround = {
                "org.apache.solr.metrics.SolrMetricsContext"
        };

        
        String[] testingClasses = {
                "com.carrotsearch.randomizedtesting.ThreadLeakControl",
                "com.carrotsearch.randomizedtesting.RandomizedRunner",
                "com.carrotsearch.randomizedtesting.rules.StatementAdapter",
                "org.apache.lucene.util.TestRule",
                "junit.framework.TestCase"
        };

        
        String[] commonLogClasses = {
                "org.slf4j.Logger",
                "org.slf4j.LoggerFactory",
                "org.apache.logging.log4j.Logger",
                "org.apache.logging.log4j.LogManager",
                "java.util.logging.Logger"
        };

        
        String[] otherClasses = {
                "java.lang.ClassLoader",
                "java.net.URLClassLoader",
                "java.net.URL",
                "java.util.regex.Pattern",
                "java.util.regex.Matcher",
                "java.io.PrintStream",
        };

        
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
                    
                }
            }
        }

        
        cloner.dontCloneInstanceOf(Function.class);
        cloner.dontCloneInstanceOf(Runnable.class);
        cloner.dontCloneInstanceOf(Callable.class);
        cloner.dontCloneInstanceOf(Consumer.class);
        cloner.dontCloneInstanceOf(Supplier.class);
        cloner.dontCloneInstanceOf(Predicate.class);
    }

    
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

                
                if (valueClassName.startsWith("sun.nio.fs.") ||
                        valueClassName.startsWith("java.nio.file.") ||
                        valueClassName.startsWith("org.apache.lucene.store.")) {
                    return true;
                }

                
                if(valueClassName.startsWith("org.apache.lucene.codecs")){
                    return true;
                }

                
                if (valueClassName.contains("Lambda") ||
                        valueClassName.contains("$Lambda") ||
                        value.getClass().isSynthetic()) {
                    return true;
                }

                
                if(valueClassName.contains("org.apache.solr") &&
                        (!valueClassName.contains("org.apache.solr.update.SolrIndexWriter"))){
                    return true;
                }

                
                if (value.getClass().isAnonymousClass()) {
                    return true;
                }

                
                if (value instanceof Thread || value instanceof ThreadGroup ||
                        value instanceof ExecutorService) {
                    return true;
                }
            }
        } catch (IllegalAccessException e) {
            
        }

        return false;
    }

    
    private static Object cloneIndexReader(Cloner cloner, Object originalObject, Map<Object, Object> clones) {
        try {
            Class<?> clazz = originalObject.getClass();

            
            Object newInstance = ObjenesisInstantiationStrategy.getInstance().newInstance(clazz);

            
            if (clones != null) {
                clones.put(originalObject, newInstance);
            }

            
            Class<?> currentClass = clazz;
            while (currentClass != null && currentClass != Object.class) {
                Field[] fields = currentClass.getDeclaredFields();
                for (Field field : fields) {
                    
                    if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }

                    field.setAccessible(true);

                    
                    if ("parentReaders".equals(field.getName())) {
                        System.out.println("AUTOPILOTHandler: Found parentReaders field, replacing with new empty Set");
                        
                        field.set(newInstance, createNewParentReadersSet());
                    } else if (shouldSkipFieldBasedOnStrategy(originalObject, field)) {
                        
                        Object fieldValue = field.get(originalObject);
                        field.set(newInstance, fieldValue);
                    } else {
                        
                        Object fieldValue = field.get(originalObject);
                        if (fieldValue != null) {
                            
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
            
            return null;
        }
    }

    
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

    
    public static HandleResult handleClone(Cloner cloner, Object originalObject, Map<Object, Object> clones) {

        
        if (isIndexReader(originalObject)) {
            System.out.println("AUTOPILOTHandler: Detected IndexReader or subclass: "
                    + originalObject.getClass().getName());

            
            Object clonedIndexReader = cloneIndexReader(cloner, originalObject, clones);

            if (clonedIndexReader != null) {
                
                return HandleResult.replaceObject(clonedIndexReader);
            } else {
                System.err.println("AUTOPILOTHandler: Failed to clone IndexReader, falling back to default cloning");
                
                return HandleResult.useDefault();
            }
        }

        
        return HandleResult.useDefault();
    }

    
    public static boolean shouldSkipField(Object object, Field field) {
        return false;
    }

    
    private static void debugPrint(String message) {
        if (System.getProperty("autopilot.debug") != null) {
            System.out.println("[AUTOPILOT DEBUG] " + message);
        }
    }
}