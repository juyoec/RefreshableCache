package com.crassirostris.cache.refresh;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * User: crassirostris
 * Date: 2015-10-14
 * Time: 오후 7:54
 */
@Configuration
@EnableCaching
@EnableAsync
@EnableScheduling
@Slf4j
public class RefreshableCacheScheduleResistrar implements SchedulingConfigurer {
	private static final String REFRESHABLE_METHOD = "processRefresh";
	@Autowired
	private ApplicationContext ac;

	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		Map<String, RefreshableCache> beansOfType = ac.getBeansOfType(RefreshableCache.class);
		for (RefreshableCache refreshableCache : beansOfType.values()) {
			Method refreshMethod = ReflectionUtils.findMethod(refreshableCache.getClass(), REFRESHABLE_METHOD);
			Runnable runnable = new ScheduledMethodRunnable(refreshableCache, refreshMethod);

			DelayType type = refreshableCache.getDelayType();

			if (DelayType.CRON_TRIGGER.equals(type)) {
				taskRegistrar.addCronTask(runnable, refreshableCache.getCronExpression());
			} else if (DelayType.FIXED_DELAY.equals(type)) {
				taskRegistrar.addFixedDelayTask(runnable, refreshableCache.getFixedInterval());
			} else if (DelayType.FIXED_RATE.equals(type)) {
				taskRegistrar.addFixedRateTask(runnable, refreshableCache.getFixedInterval());
			} else {
				log.error(String.format("cannot bind refreshableCache method %s. not match DelayType %s. It will skipped",
						refreshableCache.getClass().getCanonicalName(), type));
			}
		}
	}
}
