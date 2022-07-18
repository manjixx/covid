package cn.covid;

import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import javax.swing.*;
import javax.swing.text.html.parser.Entity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author：
 * Date：2022/5/3020:42
 * Desc:
 */
public class HttpClientTest {

    @Test
    public void testGet() throws Exception{
        // 1.创建HttpClient对象
        CloseableHttpClient httpClient = HttpClients.createDefault();
        // 2.创建HttpGet请求
        HttpGet httpGet = new HttpGet("http://www.itcast.cn/?username=java");
        httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36");
        // 3.发起请求
        // 3.发起请求
        CloseableHttpResponse response = httpClient.execute(httpGet);

        // 4.获取响应数据
        if(response.getStatusLine().getStatusCode() == 200){
            String html = EntityUtils.toString(response.getEntity(),"UTF-8");
            System.out.println(html);
        }
        // 5.关闭资源
        httpClient.close();
        response.close();
    }

    @Test
    public void testPost() throws Exception{
        // 创建HttpClient对象
        CloseableHttpClient httpClient =  HttpClients.createDefault();

        // 创建HttpPost对象并进行相关设置
        HttpPost httpPost = new HttpPost("http://www.itcast.cn/");
        // 创建集合用于存放请求参数
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("username","java"));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params,"UTF-8");
        httpPost.setEntity(entity);
        httpPost.setHeader(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36"
        );

        // 发起请求
        CloseableHttpResponse response = httpClient.execute(httpPost);
        // 判断状态响应码并获取数据
        if(response.getStatusLine().getStatusCode() == 200){
            String html = EntityUtils.toString(response.getEntity(),"UTF-8");
            System.out.println(html);
        }
        // 关闭资源
        response.close();
        httpClient.close();
    }

    @Test
    public void testPool() throws Exception{
        // 1.创建HttpClient连接池管理器
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        // 2.设置连接参数
        cm.setMaxTotal(200);    //设置最大连接数
        cm.setDefaultMaxPerRoute(20);//设置每个主机的最大并发
        doGet(cm);
        doGet(cm);
    }

    private void doGet(PoolingHttpClientConnectionManager cm) throws Exception{
        // 3.从连接池中获得HttpClient对象
        CloseableHttpClient httpClient = HttpClients.custom().setConnectionManager(cm).build();
        // 4.创建HttpGet对象
        HttpGet httpGet = new HttpGet("http://www.itcast.cn/?username=java");
        // 5. 发起请求
        CloseableHttpResponse response = httpClient.execute(httpGet);
        // 6.获取数据
        if(response.getStatusLine().getStatusCode() == 200){
            String html = EntityUtils.toString(response.getEntity(), "UTF-8");
            System.out.println(html);
        }
        // 7.关闭资源,此处不需要再次关闭HttpClient对象，用完之后需要放回连接池
        response.close();
    }

    @Test
    public void testConfit() throws Exception{
        // 0.创建请求配置对象
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(10000)    // 连接超时时间
                .setConnectTimeout(10000)   // 设置创建连接超时时间
                .setConnectionRequestTimeout(10000) //  设置请求超时时间
                .setProxy(new HttpHost("27.105.130.93",8080))
                .build();
        // 1.创建HttpClient对象
//        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
        // 2.创建HttpGet 对象
        HttpGet httpGet = new HttpGet("http://www.itcast.cn/?username=java");
        // 3.发起请求
        CloseableHttpResponse response = httpClient.execute(httpGet);
        // 4.获取数据
        if(response.getStatusLine().getStatusCode() == 200){
            String html = EntityUtils.toString(response.getEntity(),"UTF-8");
            System.out.println(html);
        }
        // 5.关闭资源
        response.close();
        httpClient.close();
    }


}
