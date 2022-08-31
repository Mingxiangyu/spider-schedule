package com.framework.admin.core.route.strategy;

import com.framework.admin.core.route.ExecutorRouter;
import com.framework.admin.core.scheduler.XxlJobScheduler;
import com.framework.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.IdleBeatParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import java.util.List;

public class ExecutorRouteBusyover extends ExecutorRouter {

  @Override
  public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
    StringBuffer idleBeatResultSB = new StringBuffer();
    for (String address : addressList) {
      // beat
      ReturnT<String> idleBeatResult = null;
      try {
        ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
        // 发送idleBeat（空闲心跳包）,检测当前机器是否空闲
        idleBeatResult = executorBiz.idleBeat(new IdleBeatParam(triggerParam.getJobId()));
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        idleBeatResult = new ReturnT<String>(ReturnT.FAIL_CODE, "" + e);
      }
      idleBeatResultSB
          .append((idleBeatResultSB.length() > 0) ? "<br><br>" : "")
          .append(I18nUtil.getString("jobconf_idleBeat") + "：")
          .append("<br>address：")
          .append(address)
          .append("<br>code：")
          .append(idleBeatResult.getCode())
          .append("<br>msg：")
          .append(idleBeatResult.getMsg());

      // EmbedServer来处理这个请求，判断当前执行器节点是否执行当前任务或者当前执行器节点的任务队列是否为空，
      // 若既不是执行当前任务的节点或者任务队列为空则返回SUCCESS
      if (idleBeatResult.getCode() == ReturnT.SUCCESS_CODE) {
        idleBeatResult.setMsg(idleBeatResultSB.toString());
        idleBeatResult.setContent(address);
        return idleBeatResult;
      }
    }

    return new ReturnT<String>(ReturnT.FAIL_CODE, idleBeatResultSB.toString());
  }
}
