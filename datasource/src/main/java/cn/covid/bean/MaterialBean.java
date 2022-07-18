package cn.covid.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Author：
 * Date：2022/5/3121:09
 * Desc: 用于封装防疫物资的JavaBean
 */
@Data
@AllArgsConstructor //生成全参数构造函数
@NoArgsConstructor //添加一个无参数的构造器
public class MaterialBean {
    private String name;    // 物资名称
    private String from;    // 物资来源
    private Integer count;  // 物资数量

}
