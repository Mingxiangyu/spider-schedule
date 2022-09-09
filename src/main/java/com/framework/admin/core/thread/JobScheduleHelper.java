package com.framework.admin.core.thread;

import com.framework.admin.core.conf.XxlJobAdminConfig;
import com.framework.admin.core.cron.CronExpression;
import com.framework.admin.core.model.XxlJobInfo;
import com.framework.admin.core.scheduler.MisfireStrategyEnum;
import com.framework.admin.core.scheduler.ScheduleTypeEnum;
import com.framework.admin.core.trigger.TriggerTypeEnum;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
  private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

  private static JobScheduleHelper instance = new JobScheduleHelper();

  public static JobScheduleHelper getInstance() {
    return instance;
  }
  /** * 预读的毫秒数 */
  public static final long PRE_READ_MS = 5000; // 提前量
  /** * 预读和调度过期任务的线程 */
  private Thread scheduleThread;

  private Thread ringThread;
  private volatile boolean scheduleThreadToStop = false;
  private volatile boolean ringThreadToStop = false;
  private static volatile Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

  public void start() {

    // schedule thread 调度线程
    scheduleThread =
        new Thread(
            () -> {
              try {
                // 随机 sleep 4000~5000 毫秒，通过这种方式，降低多实例部署时对锁的竞争
                TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis() % 1000);
              } catch (InterruptedException e) {
                if (!scheduleThreadToStop) {
                  logger.error(e.getMessage(), e);
                }
              }
              logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

              // 根据调度线程池大小动态调整任务预读数量，任务预读数量 = (快线程池数量+慢线程池数量）*20
              int preReadCount =
                  (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax()
                          + XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax())
                      * 20;

              while (!scheduleThreadToStop) {

                // 记录一个时间节点，用于后面计算耗时
                long start = System.currentTimeMillis();

                Connection conn = null;
                Boolean connAutoCommit = null;
                PreparedStatement preparedStatement = null;

                boolean preReadSuc = true;
                try {

                  conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                  // 开启事务
                  connAutoCommit = conn.getAutoCommit();
                  conn.setAutoCommit(false);
                  // 为了保证分布式一致性先上悲观锁：使用select  xx  for update来实现 避免多个xxl-job admin调度器节点同时执行
                  preparedStatement =
                      conn.prepareStatement(
                          "select * from xxl_job_lock where lock_name = 'schedule_lock' for update");
                  preparedStatement.execute();

                  // tx start

                  // 1、pre read
                  long nowTime = System.currentTimeMillis();
                  // 查询任务下一次执行时间＜当前时间+5秒的任务 最大数量为 preReadCount
                  // toDo 添加优先级排序字段，触发时优先级高的先执行
                  List<XxlJobInfo> scheduleList =
                      XxlJobAdminConfig.getAdminConfig()
                          .getXxlJobInfoDao()
                          .scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                  if (scheduleList != null && scheduleList.size() > 0) {
                    // 2、push time-ring  push压进时间轮
                    for (XxlJobInfo jobInfo : scheduleList) {
                      // 当前时间> 触发时间 + 预读时间 不在执行该任务，重新设置下一次执行时间
                      if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                        // 2.1、trigger-expire > 5s：pass && make next-trigger-time
                        logger.warn(
                            ">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());

                        // 1、获取任务的过期策略
                        MisfireStrategyEnum misfireStrategyEnum =
                            MisfireStrategyEnum.match(
                                jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                        // 1 、匹配过期失效的策略： DO_NOTHING= 过期啥也不干，废弃； FIRE_ONCE_NOW= 过期立即触发一次 misfire
                        if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum) {
                          // 如果丢失策略为过期立即执行，则执行
                          JobTriggerPoolHelper.trigger(
                              jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1, null, null, null);
                          logger.debug(
                              ">>>>>>>>>>> xxl-job, schedule push trigger : jobId = "
                                  + jobInfo.getId());
                        }

                        // 2、fresh next
                        // 刷新下次执行时间
                        refreshNextValidTime(jobInfo, new Date());
                      } else if (nowTime > jobInfo.getTriggerNextTime()) {
                        // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time
                        // 任务过期＜5秒，立即执行任务

                        // 1、trigger
                        JobTriggerPoolHelper.trigger(
                            jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
                        logger.debug(
                            ">>>>>>>>>>> xxl-job, schedule push trigger : jobId = "
                                + jobInfo.getId());

                        // 2、fresh next
                        refreshNextValidTime(jobInfo, new Date());

                        // next-trigger-time in 5s, pre-read again 如果下一次触发在五秒内，直接放进时间轮里面待调度
                        if (jobInfo.getTriggerStatus() == 1
                            && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {

                          // 1、 make ring second 求当前任务下一次触发时间所处一分钟的第 N 秒
                          int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

                          // 2、push time ring 将当前任务 ID 和 ringSecond 放进时间轮里面
                          pushTimeRing(ringSecond, jobInfo.getId());

                          // 3、fresh next
                          refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                        }

                      } else {
                        // 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time  当前时间 小于
                        // 下一次触发时间
                        // 1、make ring second
                        int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

                        // 2、push time ring
                        pushTimeRing(ringSecond, jobInfo.getId());

                        // 3、fresh next
                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                      }
                    }

                    // 3、update trigger info 更新数据库执行器信息，如 trigger_last_time 、 trigger_next_time
                    // update trigger info
                    for (XxlJobInfo jobInfo : scheduleList) {
                      XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                    }

                  } else {
                    preReadSuc = false;
                  }

                  // tx stop

                } catch (Exception e) {
                  if (!scheduleThreadToStop) {
                    logger.error(
                        ">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                  }
                } finally {
                  // 提交数据库事务
                  if (conn != null) {
                    try {
                      conn.commit();
                    } catch (SQLException e) {
                      if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                      }
                    }
                    try {
                      conn.setAutoCommit(connAutoCommit);
                    } catch (SQLException e) {
                      if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                      }
                    }
                    try {
                      conn.close();
                    } catch (SQLException e) {
                      if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                      }
                    }
                  }

                  // 释放数据库select for update排它锁
                  if (null != preparedStatement) {
                    try {
                      preparedStatement.close();
                    } catch (SQLException e) {
                      if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                      }
                    }
                  }
                }
                long cost = System.currentTimeMillis() - start;

                // 耗时超过 1000 毫秒，就不 sleep
                // 不超过 1000 ms，就 sleep 一个随机时长（执行太快了，就稍微 sleep 等待一下）
                if (cost < 1000) { // scan-overtime, not wait
                  try {
                    // pre-read period: success > scan each second; fail > skip this period;
                    // 没有预读数据，就 sleep 时间长一点；有预读数据，就 sleep 时间短一些
                    TimeUnit.MILLISECONDS.sleep(
                        (preReadSuc ? 1000 : PRE_READ_MS) - System.currentTimeMillis() % 1000);
                  } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                      logger.error(e.getMessage(), e);
                    }
                  }
                }
              }

              logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            });
    // 设置为守护线程
    scheduleThread.setDaemon(true);
    scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
    scheduleThread.start();

    // ring thread  时间轮线程，用于不断从 时间轮 中读取 当前时间点需要执行 的任务， 读取出来的任务会交给一个叫 快慢线程池 的东西去将任务传递给调度器去执行。
    ringThread =
        new Thread(
            () -> {
              while (!ringThreadToStop) {

                // align second
                try {
                  // 在执行轮询调度前，有一个时间在 0～1000 毫秒范围内的 sleep。如果没有这个 sleep，该线程会一直执行，而 ringData
                  // 中当前时刻（秒）的数据可能已经为空，会导致大量无效的操作；增加了这个 sleep 之后，可以避免这种无效的操作。之所以 sleep 时间在 1000
                  // 毫秒以内，是因为调度时刻最小精确到秒，一秒的 sleep 可以避免 job 的延迟。
                  TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                } catch (InterruptedException e) {
                  if (!ringThreadToStop) {
                    logger.error(e.getMessage(), e);
                  }
                }

                try {
                  // second data
                  List<Integer> ringItemData = new ArrayList<>();
                  int nowSecond =
                      Calendar.getInstance().get(Calendar.SECOND); // 避免处理耗时太长，跨过刻度，向前校验一个刻度；
                  // 获取当前所处的一分钟第几秒，然后 for 两次，第二次是为了重跑前面一个刻度没有被执行的的 job list ，避免前面的刻度遗漏了
                  for (int i = 0; i < 2; i++) {
                    List<Integer> tmpData = ringData.remove((nowSecond + 60 - i) % 60);
                    if (tmpData != null) {
                      ringItemData.addAll(tmpData);
                    }
                  }

                  // ring trigger
                  logger.debug(
                      ">>>>>>>>>>> xxl-job, time-ring beat : "
                          + nowSecond
                          + " = "
                          + Arrays.asList(ringItemData));
                  if (ringItemData.size() > 0) {
                    // do trigger
                    for (int jobId : ringItemData) {
                      // do trigger 执行触发器
                      JobTriggerPoolHelper.trigger(
                          jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                    }
                    // clear  清除当前刻度列表的数据
                    ringItemData.clear();
                  }
                } catch (Exception e) {
                  if (!ringThreadToStop) {
                    logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
                  }
                }
              }
              logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            });
    // 设置为守护线程
    ringThread.setDaemon(true);
    ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
    ringThread.start();
  }

  /**
   * 更新任务的 trigger_next_time 下次触发时间
   *
   * @param jobInfo
   * @param fromTime
   * @throws Exception
   */
  private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
    Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
    if (nextValidTime != null) {
      // 刷新上一次触发
      jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
      // 刷新下一次待触发时间
      jobInfo.setTriggerNextTime(nextValidTime.getTime());
    } else {
      // 获取下次执行时间失败后更新触发状态
      jobInfo.setTriggerStatus(0);
      jobInfo.setTriggerLastTime(0);
      jobInfo.setTriggerNextTime(0);
      logger.warn(
          ">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
          jobInfo.getId(),
          jobInfo.getScheduleType(),
          jobInfo.getScheduleConf());
    }
  }

  private void pushTimeRing(int ringSecond, int jobId) {
    // push async ring
    List<Integer> ringItemData = ringData.get(ringSecond);
    if (ringItemData == null) {
      ringItemData = new ArrayList<Integer>();
      ringData.put(ringSecond, ringItemData);
    }
    ringItemData.add(jobId);

    logger.debug(
        ">>>>>>>>>>> xxl-job, schedule push time-ring : "
            + ringSecond
            + " = "
            + Arrays.asList(ringItemData));
  }

  public void toStop() {

    // 1、stop schedule
    scheduleThreadToStop = true;
    try {
      TimeUnit.SECONDS.sleep(1); // wait
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }
    if (scheduleThread.getState() != Thread.State.TERMINATED) {
      // interrupt and wait
      scheduleThread.interrupt();
      try {
        scheduleThread.join();
      } catch (InterruptedException e) {
        logger.error(e.getMessage(), e);
      }
    }

    // if has ring data
    boolean hasRingData = false;
    if (!ringData.isEmpty()) {
      for (int second : ringData.keySet()) {
        List<Integer> tmpData = ringData.get(second);
        if (tmpData != null && tmpData.size() > 0) {
          hasRingData = true;
          break;
        }
      }
    }
    if (hasRingData) {
      try {
        TimeUnit.SECONDS.sleep(8);
      } catch (InterruptedException e) {
        logger.error(e.getMessage(), e);
      }
    }

    // stop ring (wait job-in-memory stop)
    ringThreadToStop = true;
    try {
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }
    if (ringThread.getState() != Thread.State.TERMINATED) {
      // interrupt and wait
      ringThread.interrupt();
      try {
        ringThread.join();
      } catch (InterruptedException e) {
        logger.error(e.getMessage(), e);
      }
    }

    logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
  }

  /**
   * 通过调度类型计算出下次调度时间，更新数据库中任务调度状态为运行，设置下次调度时间，后续即可被调度线程扫表时查询执行调度。
   *
   * @param jobInfo
   * @param fromTime
   * @return
   * @throws Exception
   */
  // ---------------------- tools ----------------------
  public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
    ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
    // 获取该任务的调度类型
    if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
      // 如果是定时调度，则通过 CronExpression 获取cron表达式下一次执行时间
      Date nextValidTime =
          new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
      return nextValidTime;
    } else if (ScheduleTypeEnum.FIX_RATE
        == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY == scheduleTypeEnum*/) {
      // 如果是固定间隔时间
      return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf()) * 1000);
    }
    return null;
  }
}
