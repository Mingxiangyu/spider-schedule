package com.framework.admin.core.thread;

import com.framework.admin.core.conf.XxlJobAdminConfig;
import com.framework.admin.core.model.XxlJobInfo;
import com.framework.admin.core.model.XxlJobLog;
import com.framework.admin.core.trigger.TriggerTypeEnum;
import com.framework.admin.core.util.I18nUtil;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * job monitor instance
 *
 * @author xuxueli 2015-9-1 18:05:56
 */
public class JobFailMonitorHelper {
  private static Logger logger = LoggerFactory.getLogger(JobFailMonitorHelper.class);

  private static JobFailMonitorHelper instance = new JobFailMonitorHelper();

  public static JobFailMonitorHelper getInstance() {
    return instance;
  }

  // ---------------------- monitor ----------------------

  private Thread monitorThread;
  private volatile boolean toStop = false;

  public void start() {
    monitorThread =
        new Thread(
            () -> {

              // monitor
              while (!toStop) {
                try {
                  // 获取执行失败的job信息
                  List<Long> failLogIds =
                      XxlJobAdminConfig.getAdminConfig()
                          .getXxlJobLogDao()
                          .findFailJobLogIds(1000);
                  if (failLogIds != null && !failLogIds.isEmpty()) {
                    for (long failLogId : failLogIds) {

                      // lock log
                      int lockRet =
                          XxlJobAdminConfig.getAdminConfig()
                              .getXxlJobLogDao()
                              .updateAlarmStatus(failLogId, 0, -1);
                      if (lockRet < 1) {
                        continue;
                      }
                      XxlJobLog log =
                          XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().load(failLogId);
                      XxlJobInfo info =
                          XxlJobAdminConfig.getAdminConfig()
                              .getXxlJobInfoDao()
                              .loadById(log.getJobId());

                      // 1、fail retry monitor 失败塞回重试
                      if (log.getExecutorFailRetryCount() > 0) {
                        JobTriggerPoolHelper.trigger(
                            log.getJobId(),
                            TriggerTypeEnum.RETRY,
                            (log.getExecutorFailRetryCount() - 1),
                            log.getExecutorShardingParam(),
                            log.getExecutorParam(),
                            null);
                        String retryMsg =
                            "<br><br><span style=\"color:#F39C12;\" > >>>>>>>>>>>"
                                + I18nUtil.getString("jobconf_trigger_type_retry")
                                + "<<<<<<<<<<< </span><br>";
                        log.setTriggerMsg(log.getTriggerMsg() + retryMsg);
                        XxlJobAdminConfig.getAdminConfig()
                            .getXxlJobLogDao()
                            .updateTriggerInfo(log);
                      }

                      // 2、fail alarm monitor 进行失败告警
                      int newAlarmStatus = 0; // 告警状态：0-默认、-1=锁定状态、1-无需告警、2-告警成功、3-告警失败
                      if (info != null) {
                        boolean alarmResult =
                            XxlJobAdminConfig.getAdminConfig().getJobAlarmer().alarm(info, log);
                        newAlarmStatus = alarmResult ? 2 : 3;
                      } else {
                        newAlarmStatus = 1;
                      }

                      XxlJobAdminConfig.getAdminConfig()
                          .getXxlJobLogDao()
                          .updateAlarmStatus(failLogId, -1, newAlarmStatus);
                    }
                  }

                } catch (Exception e) {
                  if (!toStop) {
                    logger.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e);
                  }
                }

                try {
                  TimeUnit.SECONDS.sleep(10);
                } catch (Exception e) {
                  if (!toStop) {
                    logger.error(e.getMessage(), e);
                  }
                }
              }

              logger.info(">>>>>>>>>>> xxl-job, job fail monitor thread stop");
            });
    monitorThread.setDaemon(true);
    monitorThread.setName("xxl-job, admin JobFailMonitorHelper");
    monitorThread.start();
  }

  public void toStop() {
    toStop = true;
    // interrupt and wait
    monitorThread.interrupt();
    try {
      monitorThread.join();
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }
  }
}
