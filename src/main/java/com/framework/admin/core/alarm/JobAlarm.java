package com.framework.admin.core.alarm;

import com.framework.admin.core.model.XxlJobInfo;
import com.framework.admin.core.model.XxlJobLog;

public interface JobAlarm {

  /**
   * job alarm
   *
   * @param info
   * @param jobLog
   * @return
   */
  public boolean doAlarm(XxlJobInfo info, XxlJobLog jobLog);
}
