package com.framework.admin.core.scheduler;

import com.framework.admin.core.util.I18nUtil;

/**
 * 过期策略枚举
 */
public enum MisfireStrategyEnum {

  /** 过期啥也不干 */
  DO_NOTHING(I18nUtil.getString("misfire_strategy_do_nothing")),

  /** 过期立即触发一次 */
  FIRE_ONCE_NOW(I18nUtil.getString("misfire_strategy_fire_once_now"));

  private String title;

  MisfireStrategyEnum(String title) {
    this.title = title;
  }

  public String getTitle() {
    return title;
  }

  /**
   * 根据枚举名称获取枚举项，如果没有则返回默认
   *
   * @param name 枚举名称
   * @param defaultItem 默认枚举项
   * @return
   */
  public static MisfireStrategyEnum match(String name, MisfireStrategyEnum defaultItem) {
    for (MisfireStrategyEnum item : MisfireStrategyEnum.values()) {
      if (item.name().equals(name)) {
        return item;
      }
    }
    return defaultItem;
  }
}
