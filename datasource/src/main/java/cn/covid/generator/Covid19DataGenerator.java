package cn.covid.generator;

import cn.covid.bean.MaterialBean;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * Author：
 * Date：2022/5/3121:07
 * Desc: 使用程序模拟疫情数据生成
 */
@Component
public class Covid19DataGenerator {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Scheduled(initialDelay = 1000,fixedDelay = 1000 *10)// 每隔十秒执行一次，每次生成10条数据
    public void generator(){
        Random random = new Random();
        for(int i = 0;i < 10;i++){
            MaterialBean materialBean = new MaterialBean(
                    materials[random.nextInt(materials.length)],
                    materialSources[random.nextInt(materialSources.length)],
                    random.nextInt(10000)
            );
            // 将生成的疫情物资数据转换为jsonStr再发送到Kafka集群
            String jsonStr = JSON.toJSONString(materialBean);
            kafkaTemplate.send("covid19_material",random.nextInt(3),jsonStr);
        }

    }

    // 物资名称
    private String[] materials = new String[]{
            "N95口罩/个", "医用外科口罩/个",
            "84消毒液/瓶", "电子体温计/个",
            "一次性橡胶手套/副", "防护目镜/副",
            "医用防护服/套"
    };

    // 物资来源
    private String[] materialSources = new String[]{"采购", "下拨", "捐赠", "消耗","需求"};
}
