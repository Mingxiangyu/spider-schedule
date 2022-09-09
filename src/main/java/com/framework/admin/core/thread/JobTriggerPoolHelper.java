package com.framework.admin.core.thread;

import com.framework.admin.core.conf.XxlJobAdminConfig;
import com.framework.admin.core.trigger.TriggerTypeEnum;
import com.framework.admin.core.trigger.XxlJobTrigger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * job trigger thread pool helper
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
  private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);

  // ---------------------- trigger pool ----------------------

  // fast/slow thread pool 快速、慢速线程池，分别执行调度任务不一样的任务，实现隔离，避免相互阻塞
  private ThreadPoolExecutor fastTriggerPool = null;
  private ThreadPoolExecutor slowTriggerPool = null;

  public void start() {
    // 快线程池
    fastTriggerPool =
        new ThreadPoolExecutor(
            // -corePoolSize(int)：线程池中保持的线程数量。包含空暇线程在内。也就是线程池释放的最小线程数量界限。
            10,
            // -maximumPoolSize(int):线程池中容纳最大线程数量。
            XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
            // -keepAliveTime(long):空暇线程保持在线程池中的时间。当线程池中线程数量大于corePoolSize的时候。
            60L,
            // -unit(TimeUnit枚举类):上面參数时间的单位。能够是分钟。秒，毫秒等等。
            TimeUnit.SECONDS,
            // -workQueue(BlockingQueue):任务队列，当线程任务提交到线程池以后，首先放入队列中，然后线程池依照该任务队列依次运行对应的任务。
            new LinkedBlockingQueue<Runnable>(1000),
            // -threadFactory(ThreadFactory类):新线程产生工厂类。
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                return new Thread(
                    r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
              }
            });

    // 慢线程池
    // 如果一个任务在 1 分钟内，它的执行超时次数超过 10 次，就被归为 慢任务
    slowTriggerPool =
        new ThreadPoolExecutor(
            10,
            XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(2000),
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                return new Thread(
                    r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
              }
            });
  }

  public void stop() {
    // triggerPool.shutdown();
    fastTriggerPool.shutdownNow();
    slowTriggerPool.shutdownNow();
    logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
  }

  // job timeout count
  // 属性变量，初始值等于 JobTriggerPoolHelper 对象构造时的分钟数
  // 每次调用 XxlJobTrigger.trigger() 方法时，值等于上一次调用的分钟数
  private volatile long minTim = System.currentTimeMillis() / 60000; // ms > min
  private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap =
      new ConcurrentHashMap<>();

  /** 核心方法，触发任务时调用的 */
  public void addTrigger(
      final int jobId,
      final TriggerTypeEnum triggerType,
      final int failRetryCount,
      final String executorShardingParam,
      final String executorParam,
      final String addressList) {

    // 选择线程池，默认使用快线程池，如果该任务在一分钟内超过10次，就改为使用慢线程池
    ThreadPoolExecutor triggerPool = fastTriggerPool;
    AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
    if (jobTimeoutCount != null
        && jobTimeoutCount.get() > 10) { //  job 在一分钟内超过超过 10 次，就用 slowTriggerPool 来处理
      triggerPool = slowTriggerPool;
    }

    // trigger
    triggerPool.execute(
        () -> {
          // 开始调用 XxlJobTrigger.trigger() 的时间
          long start = System.currentTimeMillis();

          try {
            // do trigger
            XxlJobTrigger.trigger(
                jobId,
                triggerType,
                failRetryCount,
                executorShardingParam,
                executorParam,
                addressList);
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
          } finally {

            // check timeout-count-map
            long minTim_now = System.currentTimeMillis() / 60000;
            // 当前时间与上次调用是否在一分钟以内，如果不在一分钟以内，就清空 map；
            if (minTim != minTim_now) {
              minTim = minTim_now;
              jobTimeoutCountMap.clear();
            }

            // 如果用时超过 500 毫秒，就增加一次它的慢调用次数
            long cost = System.currentTimeMillis() - start;
            // 本次 XxlJobTrigger.trigger() 的调用是否超过 500 毫秒，如果超过 500 毫秒，就在 map 中增加 job_id 的超时次数
            if (cost > 500) { // ob-timeout threshold 500ms
              AtomicInteger timeoutCount =
                  jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
              if (timeoutCount != null) {
                timeoutCount.incrementAndGet();
              }
            }
          }
        });
  }

  // ---------------------- helper ----------------------

  private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

  public static void toStart() {
    helper.start();
  }

  public static void toStop() {
    helper.stop();
  }

  /**
   * @param jobId
   * @param triggerType
   * @param failRetryCount >=0: use this param <0: use param from job info config
   * @param executorShardingParam
   * @param executorParam null: use job param not null: cover job param
   */
  public static void trigger(
      int jobId,
      TriggerTypeEnum triggerType,
      int failRetryCount,
      String executorShardingParam,
      String executorParam,
      String addressList) {
    helper.addTrigger(
        jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
  }
}
