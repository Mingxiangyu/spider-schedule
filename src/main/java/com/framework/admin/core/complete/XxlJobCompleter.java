package com.framework.admin.core.complete;

import com.framework.admin.core.conf.XxlJobAdminConfig;
import com.framework.admin.core.model.XxlJobInfo;
import com.framework.admin.core.model.XxlJobLog;
import com.framework.admin.core.thread.JobTriggerPoolHelper;
import com.framework.admin.core.trigger.TriggerTypeEnum;
import com.framework.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobContext;
import java.text.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xuxueli 2020-10-30 20:43:10
 */
public class XxlJobCompleter {
  private static Logger logger = LoggerFactory.getLogger(XxlJobCompleter.class);

  /**
   * common fresh handle entrance (limit only once)
   *
   * @param xxlJobLog
   * @return
   */
  public static int updateHandleInfoAndFinish(XxlJobLog xxlJobLog) {

    // finish
    finishJob(xxlJobLog);

    // text最大64kb 避免长度过长
    if (xxlJobLog.getHandleMsg().length() > 15000) {
      xxlJobLog.setHandleMsg(xxlJobLog.getHandleMsg().substring(0, 15000));
    }

    // fresh handle
    return XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateHandleInfo(xxlJobLog);
  }

  /** 如果当前任务执行成功了，就调度它的所有子任务，最后把子任务的调度消息添加到当前 job 的日志中 */
  private static void finishJob(XxlJobLog xxlJobLog) {

    // 1、handle success, to trigger child job job 执行成功，开始调度子任务
    String triggerChildMsg = null;
    if (XxlJobContext.HANDLE_COCE_SUCCESS == xxlJobLog.getHandleCode()) {
      XxlJobInfo xxlJobInfo =
          XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(xxlJobLog.getJobId());
      if (xxlJobInfo != null
          && xxlJobInfo.getChildJobId() != null
          && xxlJobInfo.getChildJobId().trim().length() > 0) {
        triggerChildMsg =
            "<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>"
                + I18nUtil.getString("jobconf_trigger_child_run")
                + "<<<<<<<<<<< </span><br>";

        String[] childJobIds = xxlJobInfo.getChildJobId().split(",");
        // 遍历子任务ID
        for (int i = 0; i < childJobIds.length; i++) {
          int childJobId =
              (childJobIds[i] != null
                      && childJobIds[i].trim().length() > 0
                      && isNumeric(childJobIds[i]))
                  ? Integer.valueOf(childJobIds[i])
                  : -1;
          if (childJobId > 0) {
            // 调度子任务
            JobTriggerPoolHelper.trigger(childJobId, TriggerTypeEnum.PARENT, -1, null, null, null);
            ReturnT<String> triggerChildResult = ReturnT.SUCCESS;

            // add msg
            triggerChildMsg +=
                MessageFormat.format(
                    I18nUtil.getString("jobconf_callback_child_msg1"),
                    (i + 1),
                    childJobIds.length,
                    childJobIds[i],
                    (triggerChildResult.getCode() == ReturnT.SUCCESS_CODE
                        ? I18nUtil.getString("system_success")
                        : I18nUtil.getString("system_fail")),
                    triggerChildResult.getMsg());
          } else {
            triggerChildMsg +=
                MessageFormat.format(
                    I18nUtil.getString("jobconf_callback_child_msg2"),
                    (i + 1),
                    childJobIds.length,
                    childJobIds[i]);
          }
        }
      }
    }

    if (triggerChildMsg != null) {
      xxlJobLog.setHandleMsg(xxlJobLog.getHandleMsg() + triggerChildMsg);
    }

    // 2、fix_delay trigger next
    // on the way

  }

  private static boolean isNumeric(String str) {
    try {
      int result = Integer.valueOf(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
