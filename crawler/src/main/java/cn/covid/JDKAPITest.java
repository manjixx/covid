package cn.covid;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Author：
 * Date：2022/5/3020:25
 * Desc: 使用JDK自带API实现网络爬虫
 */

public class JDKAPITest {
    @Test
    public void testGet() throws Exception{
        //1.确定首页的URL
        URL url = new URL("http://www.itcast.cn/?username=zhangsan");

        //2.通过url对象获取远程连接
        HttpURLConnection urlConnection = (HttpURLConnection)url.openConnection();

        //3.设置请求方式  请求参数  请求头
        urlConnection.setRequestMethod("GET");  //设置请求方式的时候一定要大写, 默认的请求方式是GET
        urlConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36");
        urlConnection.setConnectTimeout(30000); //连接超时 单位毫秒
        urlConnection.setReadTimeout(30000);   //读取超时 单位毫秒

        //4.获取数据
        InputStream in = urlConnection.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        String html = "";
        while ((line = reader.readLine()) != null) {
            html +=  line + "\n";
        }
        System.out.println(html);
        in.close();
        reader.close();
    }

    @Test
    public void testPost() throws Exception{
        //1.确定首页的URL
        URL url = new URL("http://www.itcast.cn");

        //2.获取远程连接
        HttpURLConnection urlConnection =(HttpURLConnection) url.openConnection();

        //3.设置请求方式  请求参数  请求头
        urlConnection.setDoOutput(true); // 原生jdk默认关闭了输出流
        urlConnection.setRequestMethod("POST");
        urlConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36");
        urlConnection.setConnectTimeout(30000); //连接超时 单位毫秒
        OutputStream out = urlConnection.getOutputStream();
        out.write("username=xx".getBytes());

        //4.获取数据
        InputStream in = urlConnection.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        String html = "";
        while ((line = reader.readLine()) != null) {
            html +=  line + "\n";
        }
        System.out.println(html);
        in.close();
        reader.close();
    }
}
