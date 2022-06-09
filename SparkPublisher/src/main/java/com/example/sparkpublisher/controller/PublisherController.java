package com.example.sparkpublisher.controller;

import com.example.sparkpublisher.bean.NameValue;
import com.example.sparkpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 控制层
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    /**
     * 交易分析 - 按照类别（年龄、性别）统计
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2022-05-22&t=age
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2022-05-22&t=gender
     */
    @GetMapping("statsByItem")
    public List<NameValue> statsByItem(@RequestParam("itemName") String itemName, @RequestParam("date") String date, @RequestParam("t") String t) {
        List<NameValue> result = publisherService.doStatsByItem(itemName, date, t);
        return result;
    }

    /**
     * 交易分析 - 明细
     * http://bigdata.gmall.com/detailByItem?date=2022-05-22&itemName=小米手机&pageNo=1&pageSize=20
     */
    @GetMapping("detailByItem")
    public Map<String, Object> detailByItem(@RequestParam("date") String date, @RequestParam("itemName") String itemName, @RequestParam(value = "pageNo", required = false, defaultValue = "1") Integer pageNo, @RequestParam(value = "pageSize", required = false, defaultValue = "20") Integer pageSize) {
        Map<String, Object> results = publisherService.doDetailByItem(date, itemName, pageNo, pageSize);
        return results;
    }

    /**
     * 日活分析
     *
     * @param td
     * @return
     */
    @GetMapping("dauRealtime")
    public Map<String, Object> dauRealtime(@RequestParam("td") String td) {
        Map<String, Object> result = publisherService.doDauRealtime(td);
        return result;
    }
}
