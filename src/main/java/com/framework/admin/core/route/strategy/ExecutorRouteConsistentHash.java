package com.framework.admin.core.route.strategy;

import com.framework.admin.core.route.ExecutorRouter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 分组下机器地址相同，不同JOB均匀散列在不同机器上，保证分组下机器分配JOB平均；且每个JOB固定调度其中一台机器；
 * a、virtual node：解决不均衡问题 b、hash method
 * replace hashCode：String的hashCode可能重复，需要进一步扩大hashCode的取值范围
 */
public class ExecutorRouteConsistentHash extends ExecutorRouter {

  private static int VIRTUAL_NODE_NUM = 100;

  /**
   * get hash code on 2^32 ring (md5散列的方式计算hash值)
   *
   * @param key
   * @return
   */
  private static long hash(String key) {

    // md5 byte
    MessageDigest md5;
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 not supported", e);
    }
    md5.reset();
    byte[] keyBytes = null;
    try {
      keyBytes = key.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Unknown string :" + key, e);
    }

    md5.update(keyBytes);
    byte[] digest = md5.digest();

    // hash code, Truncate to 32-bits
    long hashCode =
        ((long) (digest[3] & 0xFF) << 24)
            | ((long) (digest[2] & 0xFF) << 16)
            | ((long) (digest[1] & 0xFF) << 8)
            | (digest[0] & 0xFF);
    // 通过md5算出的hashcode % 2^32 余数，将hash值散列在一致性hash环上  这个环分了2^32个位置
    long truncateHashCode = hashCode & 0xffffffffL;
    return truncateHashCode;
  }

  public String hashJob(int jobId, List<String> addressList) {

    // ------A1------A2-------A3------
    // -----------J1------------------
    TreeMap<Long, String> addressRing = new TreeMap<Long, String>();
    for (String address : addressList) {
      for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
        //为每一个注册的节点分配100个虚拟节点，并算出这些节点的一致性hash值，存放到TreeMap中
        long addressHash = hash("SHARD-" + address + "-NODE-" + i);
        addressRing.put(addressHash, address);
      }
    }
    //第二步求出job的hash值 通过jobId计算
    long jobHash = hash(String.valueOf(jobId));
    //通过treeMap性质，所有的key都按照从小到大的排序，即按照hash值从小到大排序,通过tailMap 求出>=hash(jobId)的剩余一部分map，
    SortedMap<Long, String> lastRing = addressRing.tailMap(jobHash);
    if (!lastRing.isEmpty()) {
      //若找到则取第一个key，为带路由的地址
      return lastRing.get(lastRing.firstKey());
    }
    //若本身hash(jobId)为treeMap的最后一个key，则找当前treeMap的第一个key
    return addressRing.firstEntry().getValue();
  }

  @Override
  public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
    String address = hashJob(triggerParam.getJobId(), addressList);
    return new ReturnT<>(address);
  }
}
