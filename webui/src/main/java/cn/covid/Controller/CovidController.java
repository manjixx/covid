package cn.covid.controller;

import cn.covid.bean.Result;
import cn.covid.mapper.CovidMapper;
import org.apache.commons.lang.time.FastDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;
import java.util.Map;

/**
 * Author：
 * Date：2022/7/2122:11
 * Desc:
 */
public class CovidController {
    @Autowired
    private CovidMapper covidMapper;

    @RequestMapping("getCovidWz")
    public Result getCovidWz(){
        List<Map<String, Object>> data = covidMapper.getCovidWz();
        Result result = Result.success(data);
        return result;
    }

    @RequestMapping("getNationalData")
    public Result getNationalData(){
        String datetime = FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis());
        //datetime = "2020-05-19";
        List<Map<String, Object>> data  = covidMapper.getNationalData(datetime);
        Map<String, Object> map = data.get(0);
        Result result = Result.success(map);
        return result;
    }

    @RequestMapping("getNationalMapData")
    public Result getNationalMapData(){
        String datetime = FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis());
        //datetime = "2020-05-15";
        List<Map<String, Object>> data  = covidMapper.getNationalMapData(datetime);
        Result result = Result.success(data);
        return result;
    }

    @RequestMapping("getCovidTimeData")
    public Result getCovidTimeData(){
        List<Map<String, Object>> data = covidMapper.getCovidTimeData();
        Result result = Result.success(data);
        return result;
    }

    @RequestMapping("getCovidImportData")
    public Result getCovidImportData(){
        String datetime = FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis());
        List<Map<String, Object>> data = covidMapper.getCovidImportData(datetime);
        Result result = Result.success(data);
        return result;
    }

}
