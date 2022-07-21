package cn.covid.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author：
 * Date：2022/7/2122:11
 * Desc:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor

public class Result {
    private Integer code;
    private String message;
    private Object data;

    public static Result success(Object data) {
        Result result = new Result();
        result.setCode(200);
        result.setMessage("success");
        result.setData(data);
        return result;
    }

    public static Result fail() {
        Result result = new Result();
        result.setCode(400);
        result.setMessage("fail");
        result.setData(null);
        return result;
    }

}
