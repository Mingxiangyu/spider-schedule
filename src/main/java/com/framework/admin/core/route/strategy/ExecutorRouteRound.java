package com.framework.admin.core.route.strategy;

import com.framework.admin.core.route.ExecutorRouter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** 轮询选取 */
public class ExecutorRouteRound extends ExecutorRouter {

  private static ConcurrentMap<Integer, AtomicInteger> routeCountEachJob =
      new ConcurrentHashMap<>();
  private static long CACHE_VALID_TIME = 0;

  private static int count(int jobId) {
    // cache clear 每隔24小时进行一次缓存清除
    if (System.currentTimeMillis() > CACHE_VALID_TIME) {
      routeCountEachJob.clear();
      CACHE_VALID_TIME = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
    }

    AtomicInteger count = routeCountEachJob.get(jobId);
    if (count == null || count.get() > 1000000) {
      // 初始化时主动Random一次，缓解首次压力
      count = new AtomicInteger(new Random().nextInt(100));
    } else {
      // count++
      count.addAndGet(1);
    }
    routeCountEachJob.put(jobId, count);
    return count.get();
  }

  @Override
  public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
    String address = addressList.get(count(triggerParam.getJobId()) % addressList.size());
    return new ReturnT<String>(address);
  }
}
