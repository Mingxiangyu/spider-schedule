package com.framework.admin.core.scheduler;

import com.framework.admin.core.conf.XxlJobAdminConfig;
import com.framework.admin.core.thread.JobCompleteHelper;
import com.framework.admin.core.thread.JobFailMonitorHelper;
import com.framework.admin.core.thread.JobLogReportHelper;
import com.framework.admin.core.thread.JobRegistryHelper;
import com.framework.admin.core.thread.JobScheduleHelper;
import com.framework.admin.core.thread.JobTriggerPoolHelper;
import com.framework.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */
public class XxlJobScheduler {
  private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);

  public void init() throws Exception {
    // 初始化i18n（国际化）
    initI18n();

    // start定时线程池（触发器线程池） 初始化快线程池 fastTriggerPool 、慢线程池 slowTriggerPool
    JobTriggerPoolHelper.toStart();

    // 注册器的启动 (其实就用一个线程去执行registry，就是你定义的registry会30s注册一次，
    // 注册的时候就会改xxl_job_registry表的updateTime，如果3次都没注册，就把这个服务剔除，这种操作)
    JobRegistryHelper.getInstance().start();

    // 失败后的注册器的启动(其实就是失败任务执行了，就会回调任务中心，然后这些失败的数据会做一些处理，比如说重新执行啊之类的)
    JobFailMonitorHelper.getInstance().start();

    // 丢失任务的处理的启动(其实就是说，对丢失任务的处理，源码是这么给的注释：
    // 任务结果丢失处理：调度记录停留在 “运行中” 状态超过10min，且对应执行器心跳注册失败不在线，则将本地调度主动标记失败)
    JobCompleteHelper.getInstance().start();

    // 日志的记录。打开源码就会发现，是一个while(!toStop),就是说，如果xxl-job不停止，就会一直不断的执行日志相关
    JobLogReportHelper.getInstance().start();

    // 1、新建一个线程，停了5s以后，去查数据库(select * from xxl_job_lock where lock_name = ‘schedule_lock’ for
    //  update),其实就是一个加锁的操作。
    // 2、当前时间+提前量(默认是5s)去查(xxl_job_info,筛选条件是 trigger_next_time 下次调度时间<= 当前时间+提前量
    //  数据，并拿到任务数据。(假设我有一个定时器，是说9点整去执行某个东西，我总不能9点整再去从库中查吧，因为我的调度中心是解耦出来的，
    //  调度中心查数据库、调度中心将数据给执行器，都是有开销的，所以得提前去查出来任务，这个提前量默认是是5s) 拿到数据以后，
    //  将任务放在 ringThread (他是一个ConcurrentHashMap对象)中的。
    // 3、JobScheduleHelper(任务管理器)做的工作：
    //  第一个线程 scheduleThread 扫描任务线程：
    //  a、获取任务调度lock
    //  b、查询5s内需要执行的任务
    //  c、构建待执行任务 ringItemData
    //  第二个线程 ringThread (循环处理任务线程)：
    //  a、 构建触发器 JobTriggerPoolHelper
    //  b、 triggerPool_.execute
    //  c、 根据路由规则获取调度地址
    //  d、 RPC调用
    // 原文链接：https://blog.csdn.net/weixin_42351206/article/details/125082485
    JobScheduleHelper.getInstance().start();

    logger.info(">>>>>>>>> init xxl-job admin success.");
  }

  public void destroy() throws Exception {

    // stop-schedule
    JobScheduleHelper.getInstance().toStop();

    // admin log report stop
    JobLogReportHelper.getInstance().toStop();

    // admin lose-monitor stop
    JobCompleteHelper.getInstance().toStop();

    // admin fail-monitor stop
    JobFailMonitorHelper.getInstance().toStop();

    // admin registry stop
    JobRegistryHelper.getInstance().toStop();

    // admin trigger pool stop
    JobTriggerPoolHelper.toStop();
  }

  // ---------------------- I18n ----------------------

  private void initI18n() {
    for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
      item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
    }
  }

  // ---------------------- executor-client ----------------------
  private static ConcurrentMap<String, ExecutorBiz> executorBizRepository =
      new ConcurrentHashMap<String, ExecutorBiz>();

  public static ExecutorBiz getExecutorBiz(String address) throws Exception {
    // valid
    if (address == null || address.trim().length() == 0) {
      return null;
    }

    // load-cache
    address = address.trim();
    ExecutorBiz executorBiz = executorBizRepository.get(address);
    if (executorBiz != null) {
      return executorBiz;
    }

    // set-cache
    executorBiz =
        new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

    executorBizRepository.put(address, executorBiz);
    return executorBiz;
  }
}
