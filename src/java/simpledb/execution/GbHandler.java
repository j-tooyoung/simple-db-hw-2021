package simpledb.execution;

import simpledb.storage.Field;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class GbHandler {

    Map<String, Integer> gbResult;

    /**
     * @param key   group by 字段
     * @param field 聚集函数计算字段
     */
    abstract void handle(String key, Field field);

    public GbHandler() {
        gbResult = new ConcurrentHashMap<>();
    }

    public Map<String, Integer> getGbResult() {
        return gbResult;
    }
}

class MinHandler extends GbHandler {
    @Override
    void handle(String key, Field field) {
        int val = Integer.parseInt(field.toString());
        if (gbResult.containsKey(key)) {
            gbResult.put(key, Math.min(val, gbResult.get(key)));
        } else {
            gbResult.put(key, val);
        }
    }
}

class MaxHandler extends GbHandler {

    @Override
    void handle(String key, Field field) {
        int val = Integer.parseInt(field.toString());
        if (gbResult.containsKey(key)) {
            gbResult.put(key, Math.max(val, gbResult.get(key)));
        } else {
            gbResult.put(key, val);
        }
    }
}

class AvgHandler extends GbHandler {

    private Map<String, Integer> sumMap;
    private Map<String, Integer> cntMap;

    AvgHandler() {
        this.sumMap = new ConcurrentHashMap<>();
        this.cntMap = new ConcurrentHashMap<>();
    }

    @Override
    void handle(String key, Field field) {
        int val = Integer.parseInt(field.toString());
        if (sumMap.containsKey(key)) {
            sumMap.put(key, val + sumMap.get(key));
        } else {
            sumMap.put(key, val);
        }
        cntMap.put(key, cntMap.getOrDefault(key, 0) + 1);
        gbResult.put(key, sumMap.get(key) / cntMap.get(key));
    }
}

class SumHandler extends GbHandler {

    @Override
    void handle(String key, Field field) {
        int val = Integer.parseInt(field.toString());
        if (gbResult.containsKey(key)) {
            gbResult.put(key, val + gbResult.get(key));
        } else {
            gbResult.put(key, val);
        }
    }
}

class SumCountHandler extends GbHandler {

    @Override
    void handle(String key, Field field) {

    }
}

class ScAvgHandler extends GbHandler {

    @Override
    void handle(String key, Field field) {

    }
}

class CountHandler extends GbHandler {

    @Override
    void handle(String key, Field field) {
        gbResult.put(key, gbResult.getOrDefault(key, 0) + 1);
    }
}

