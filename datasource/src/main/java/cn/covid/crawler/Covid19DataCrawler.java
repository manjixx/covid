package cn.covid.crawler;

import cn.covid.bean.CovidBean;
import cn.covid.utils.HttpUtils;
import cn.covid.utils.TimeUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author：
 * Date：2022/5/3115:59
 * Desc: 实现疫情数据爬取
 */
@Component
public class Covid19DataCrawler {
    @Autowired
    private KafkaTemplate kafkaTemplate;
    // 后续需要将该方法改为定时任务，如每天8点定时排序疫情数据
    // @Scheduled(cron = "0/5 * * * * ?")  // 每隔1s执行
    // @Scheduled(cron = "0 0 8 * * ?")  // 每天的8点定时执行
    @Scheduled(initialDelay = 1000,fixedDelay = 1000*60*60*24)
    public void testCrawling() throws Exception{
        String datetime = TimeUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
        // 1.爬取指定页面
        String html = HttpUtils.getHtml("https://ncov.dxy.cn/ncovh5/view/pneumonia");

        // 2.解析页面内容-即id为getAreaStat的标签中的全国疫情数据
        Document doc = Jsoup.parse(html);
        String text = doc.select("script[id = getAreaStat]").toString();

        // 3.使用正则表达式获取json格式的疫情数据
        String pattern ="\\[(.*)\\]";       // 定义正则规则
        Pattern reg = Pattern.compile(pattern); //编译成正则对象
        Matcher matcher = reg.matcher(text);    // 去text中进行匹配
        String jsonStr = "";
        if(matcher.find()){// 如果text中的内容和正则规则匹配上就取出来给jsonStr
            jsonStr = matcher.group(0);
        }

        // 对json数据进行进一步解析
        // 4.将第一层json(省份数据)解析为JavaBean
        List<CovidBean> proCovidBeans = JSON.parseArray(jsonStr, CovidBean.class);
        for(CovidBean proCovidBean : proCovidBeans){    // 省份
            // 设置数据爬取时间
            proCovidBean.setDatetime(datetime);
            // 获取cities
            String cityStr = proCovidBean.getCities();
            // 5.将第二层json，即城市数据解析为JavaBean
            List<CovidBean> covidBeans = JSON.parseArray(cityStr, CovidBean.class);
            for(CovidBean bean:covidBeans){    //城市
                bean.setDatetime(datetime);
                bean.setPid(proCovidBean.getLocationId());  //将省份的id作为城市的pid
                bean.setProvinceName(proCovidBean.getProvinceName());
                bean.setProvinceShortName(proCovidBean.getProvinceShortName());
                // 后续需要将城市数据发送给kafka
                // 需要将JAVABean转为JSONStr再发给Kafka
                String beanStr = JSON.toJSONString(bean);
                kafkaTemplate.send("covid19",bean.getPid(),beanStr);
//                kafkaTemplate.send("covid19",bean.getPid(),beanStr);
            }
            // 6.获取第一层json(省份数据)中每一天的统计数据
            String statisticDataUrl = proCovidBean.getStatisticsData();
            String statisticDataStr = HttpUtils.getHtml(statisticDataUrl);
            // 获取statisticDataStr中的Data对应的字段
            JSONObject jsonObject = JSON.parseObject(statisticDataStr);
            String dataStr = jsonObject.getString("data");
            // 7. 将每一天的数据设置回省份proBean中的StaticData字段中(该字段之前只是一个URL)
            proCovidBean.setStatisticsData(dataStr);
            proCovidBean.setCities(null);
            // 后续需要将省份数据发送给kafka
            String proCovidBeanStr = JSON.toJSONString(proCovidBean);
            kafkaTemplate.send("covid19",proCovidBean.getLocationId(),proCovidBeanStr);
        }
    }
}
