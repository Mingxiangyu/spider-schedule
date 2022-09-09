package com.framework.admin.core.thread;

import com.framework.admin.core.conf.XxlJobAdminConfig;
import com.framework.admin.core.model.XxlJobGroup;
import com.framework.admin.core.model.XxlJobRegistry;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 心跳检测线程池
 *
 * @author xuxueli 2016-10-02 19:10:24
 */
public class JobRegistryHelper {
  private static Logger logger = LoggerFactory.getLogger(JobRegistryHelper.class);

  private static JobRegistryHelper instance = new JobRegistryHelper();

  public static JobRegistryHelper getInstance() {
    return instance;
  }

  private ThreadPoolExecutor registryOrRemoveThreadPool = null;
  // 注册监视器线程
  private Thread registryMonitorThread;
  private volatile boolean toStop = false;

  public void start() {

    // for registry or remove 用于注册或者移除的线程池，客户端调用api/registry或api/registryRemove接口时，会用这个线程池进行注册或注销
    registryOrRemoveThreadPool =
        // 线程池的核心线程数是 2，最大线程数是10，允许一个 2000 的队列，如果 executor 实例很多，会导致注册延迟的。当然，一般不会把2000个 executor
        // 注册到同一个admin 服务。
        new ThreadPoolExecutor(
            2,
            10,
            30L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(2000),
            r ->
                new Thread(
                    r,
                    "xxl-job, admin JobRegistryMonitorHelper-registryOrRemoveThreadPool-"
                        + r.hashCode()),
            (r, executor) -> {
              r.run();
              logger.warn(
                  ">>>>>>>>>>> xxl-job, registry or remove too fast, match threadpool rejected handler(run now).");
            });

    // toDo 添加线程进行获取注册的执行器
    // for monitor 启动监听注册的线程
    registryMonitorThread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                while (!toStop) {
                  try {
                    // auto registry group 获取自动注册的执行器组（执行器地址类型：0=自动注册、1=手动录入）
                    List<XxlJobGroup> groupList =
                        XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().findByAddressType(0);
                    if (groupList != null && !groupList.isEmpty()) { // group组集合不为空

                      // remove dead address (admin/executor)
                      // 移除死掉的调用地址（心跳时间超过90秒，就当线程挂掉了。默认是30s做一次心跳）
                      List<Integer> ids =
                          XxlJobAdminConfig.getAdminConfig()
                              .getXxlJobRegistryDao()
                              .findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
                      if (ids != null && ids.size() > 0) { // 移除挂掉的注册地址信息
                        XxlJobAdminConfig.getAdminConfig().getXxlJobRegistryDao().removeDead(ids);
                      }

                      // fresh online address (admin/executor) 找出所有正常没死掉的注册地址
                      HashMap<String, List<String>> appAddressMap = new HashMap<>(16);
                      List<XxlJobRegistry> list =
                          XxlJobAdminConfig.getAdminConfig()
                              .getXxlJobRegistryDao()
                              .findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
                      if (list != null) {
                        for (XxlJobRegistry item : list) {
                          // 确保是 EXECUTOR 执行器类型
                          if (RegistryConfig.RegistType.EXECUTOR
                              .name()
                              .equals(item.getRegistryGroup())) {
                            String appname = item.getRegistryKey();
                            List<String> registryList = appAddressMap.get(appname);
                            if (registryList == null) {
                              registryList = new ArrayList<String>();
                            }

                            if (!registryList.contains(item.getRegistryValue())) {
                              registryList.add(item.getRegistryValue());
                            }
                            appAddressMap.put(appname, registryList);
                          }
                        }
                      }

                      // fresh group address 刷新分组注册地址信息
                      for (XxlJobGroup group : groupList) {
                        List<String> registryList = appAddressMap.get(group.getAppname());
                        String addressListStr = null;
                        if (registryList != null && !registryList.isEmpty()) {
                          Collections.sort(registryList);
                          StringBuilder addressListSB = new StringBuilder();
                          for (String item : registryList) {
                            addressListSB.append(item).append(",");
                          }
                          addressListStr = addressListSB.toString();
                          addressListStr = addressListStr.substring(0, addressListStr.length() - 1);
                        }
                        group.setAddressList(addressListStr);
                        group.setUpdateTime(new Date());

                        XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().update(group);
                      }
                    }
                  } catch (Exception e) {
                    if (!toStop) {
                      logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                    }
                  }
                  try {
                    // 每 30 秒执行一次
                    TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                  } catch (InterruptedException e) {
                    if (!toStop) {
                      logger.error(">>>>>>>>>>> xxl-job, job registry monitor thread error:{}", e);
                    }
                  }
                }
                logger.info(">>>>>>>>>>> xxl-job, job registry monitor thread stop");
              }
            });
    registryMonitorThread.setDaemon(true);
    registryMonitorThread.setName("xxl-job, admin JobRegistryMonitorHelper-registryMonitorThread");
    registryMonitorThread.start();
  }

  public void toStop() {
    toStop = true;

    // stop registryOrRemoveThreadPool
    registryOrRemoveThreadPool.shutdownNow();

    // stop monitir (interrupt and wait)
    registryMonitorThread.interrupt();
    try {
      registryMonitorThread.join();
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    }
  }

  // ---------------------- helper ----------------------

  public ReturnT<String> registry(RegistryParam registryParam) {

    // 校验参数
    if (!StringUtils.hasText(registryParam.getRegistryGroup())
        || !StringUtils.hasText(registryParam.getRegistryKey())
        || !StringUtils.hasText(registryParam.getRegistryValue())) {
      return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
    }

    // 在线程池中创建线程
    registryOrRemoveThreadPool.execute(
        new Runnable() {
          @Override
          public void run() {
            // update
            int ret =
                XxlJobAdminConfig.getAdminConfig()
                    .getXxlJobRegistryDao()
                    .registryUpdate(
                        registryParam.getRegistryGroup(),
                        registryParam.getRegistryKey(),
                        registryParam.getRegistryValue(),
                        new Date());
            // update 失败，insert
            if (ret < 1) {
              XxlJobAdminConfig.getAdminConfig()
                  .getXxlJobRegistryDao()
                  .registrySave(
                      registryParam.getRegistryGroup(),
                      registryParam.getRegistryKey(),
                      registryParam.getRegistryValue(),
                      new Date());

              // fresh
              freshGroupRegistryInfo(registryParam);
            }
          }
        });

    return ReturnT.SUCCESS;
  }

  public ReturnT<String> registryRemove(RegistryParam registryParam) {

    // valid
    if (!StringUtils.hasText(registryParam.getRegistryGroup())
        || !StringUtils.hasText(registryParam.getRegistryKey())
        || !StringUtils.hasText(registryParam.getRegistryValue())) {
      return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
    }

    // async execute
    registryOrRemoveThreadPool.execute(
        new Runnable() {
          @Override
          public void run() {
            int ret =
                XxlJobAdminConfig.getAdminConfig()
                    .getXxlJobRegistryDao()
                    .registryDelete(
                        registryParam.getRegistryGroup(),
                        registryParam.getRegistryKey(),
                        registryParam.getRegistryValue());
            if (ret > 0) {
              // fresh
              freshGroupRegistryInfo(registryParam);
            }
          }
        });

    return ReturnT.SUCCESS;
  }

  private void freshGroupRegistryInfo(RegistryParam registryParam) {
    // Under consideration, prevent affecting core tables
  }
}
