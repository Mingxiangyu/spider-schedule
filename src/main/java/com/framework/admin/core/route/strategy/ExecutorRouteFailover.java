package com.framework.admin.core.route.strategy;

import com.framework.admin.core.route.ExecutorRouter;
import com.framework.admin.core.scheduler.XxlJobScheduler;
import com.framework.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import java.util.List;

/** 故障转移路由策略 思想：遍历所有的该组下的所有注册节点地址集合，然后分别进行心跳处理，直到找到一个发送心跳成功的节点作为下一次路由的节点 */
public class ExecutorRouteFailover extends ExecutorRouter {

  @Override
  public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {

    StringBuffer beatResultSB = new StringBuffer();
    for (String address : addressList) {
      // beat
      ReturnT<String> beatResult = null;
      try {
        ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
        beatResult = executorBiz.beat();
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        beatResult = new ReturnT<String>(ReturnT.FAIL_CODE, "" + e);
      }
      beatResultSB
          .append((beatResultSB.length() > 0) ? "<br><br>" : "")
          .append(I18nUtil.getString("jobconf_beat") + "：")
          .append("<br>address：")
          .append(address)
          .append("<br>code：")
          .append(beatResult.getCode())
          .append("<br>msg：")
          .append(beatResult.getMsg());

      // beat success
      if (beatResult.getCode() == ReturnT.SUCCESS_CODE) {

        beatResult.setMsg(beatResultSB.toString());
        beatResult.setContent(address);
        return beatResult;
      }
    }
    return new ReturnT<String>(ReturnT.FAIL_CODE, beatResultSB.toString());
  }
}
