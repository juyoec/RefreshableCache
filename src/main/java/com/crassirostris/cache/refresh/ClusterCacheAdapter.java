package com.crassirostris.cache.refresh;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * User: crassirostris
 * Date: 2015-11-06
 * Time: 오후 4:04
 */
@RestController
public class ClusterCacheAdapter {
	public static final String REQUEST_URI = "/clusterget";
	@Autowired
	private CacheManager cacheManager;

	@RequestMapping("/clusterping/{name}/{key}")
	public String receivePing(@PathVariable("name") String name, @PathVariable("key") String keyString, @RequestParam("host") String host) {
		int key = Integer.parseInt(keyString);
		ClusterCache cache = getClusterCache(name);
		cache.receivePing(host, key);
		return "OK";
	}

	@RequestMapping( REQUEST_URI + "/{name}/{key}")
	public Object getData(@PathVariable("name") String name, @PathVariable("key") String key) {
		ClusterCache cache = getClusterCache(name);
		Cache.ValueWrapper valueWrapper = cache.get(key);
		Object o = valueWrapper.get();
		return o;
	}

	private ClusterCache getClusterCache(String name) {
		Cache cache = cacheManager.getCache(name);
		ClusterCache clusterCache = (ClusterCache) cache;
		return clusterCache;
	}
}
