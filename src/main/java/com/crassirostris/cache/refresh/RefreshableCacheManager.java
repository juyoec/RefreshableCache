package com.crassirostris.cache.refresh;

import com.google.common.collect.Lists;
import org.springframework.cache.Cache;
import org.springframework.cache.guava.GuavaCacheManager;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * User: crassirostris
 * Date: 2015-10-14
 * Time: 오후 6:27
 */
public class RefreshableCacheManager extends GuavaCacheManager {
	// private override
	private final ConcurrentMap<String, Cache> cacheMap = new ConcurrentHashMap<String, Cache>(16);

	public void setCaches(Cache... caches) {
		for (Cache cach : caches) {
			this.cacheMap.put(cach.getName(), cach);
		}
	}

	@Override
	public Collection<String> getCacheNames() {
		List<String> strings = Lists.newArrayList(cacheMap.keySet());
		strings.addAll(super.getCacheNames());
		Collection<String> cacheNames = Collections.unmodifiableCollection(strings);
		return cacheNames;
	}

	@Override
	public Cache getCache(String name) {
		if (this.cacheMap.containsKey(name)) {
			return this.cacheMap.get(name);
		}

		return super.getCache(name);
	}
}
