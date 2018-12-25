package com.github.aaric.achieve.obs;

import com.obs.services.ObsClient;
import com.obs.services.model.*;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

/**
 * ObsTest
 *
 * @author Aaric, created on 2018-12-07T16:49.
 * @since 0.0.1-SNAPSHOT
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class ObsTest {

    @Value("${huawei.obs.endPoint}")
    private String endPoint;

    @Value("${huawei.obs.accessKeyId}")
    private String accessKeyId;

    @Value("${huawei.obs.secretAccessKey}")
    private String secretAccessKey;

    @Value("${huawei.obs.bucketName}")
    private String bucketName;

    /**
     * 测试文件
     */
    private static final String testFileDirectory = "C:\\Users\\root\\Desktop\\";
    private static final String testFileName = "404.jpg";

    protected ObsClient client;

    @Before
    public void begin() {
        // 创建ObsClient实例
        client = new ObsClient(accessKeyId, secretAccessKey, endPoint);
    }

    @After
    public void end() throws IOException {
        // 关闭obsClient
        client.close();
    }

    @Test
    @Ignore
    public void testConnect() {
        System.out.println(endPoint);
        System.out.println(accessKeyId);
        System.out.println(secretAccessKey);

        // 使用访问OBS
        // TODO 对OBS进行业务操作
    }

    @Test
    @Ignore
    public void testCreateBucket() {
        // 判断桶是否存在
        String bucketName = "bucketname";
        boolean exists = client.headBucket(bucketName);

        // 简单创建一个桶（无权限）
        if (exists) {
            client.createBucket(bucketName);
        }

        // 创建一个归档类型的桶（无权限）
        if (exists) {
            ObsBucket obsBucket = new ObsBucket();
            obsBucket.setBucketName(bucketName);
            // 设置桶访问权限为公共读，默认是私有读写
            obsBucket.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
            // 设置桶的存储类型为归档存储
            obsBucket.setBucketStorageClass(StorageClassEnum.COLD);
            // 设置桶区域位置
            obsBucket.setLocation("bucketlocation");
            // 创建桶
            client.createBucket(obsBucket);
        }
    }

    @Test
    @Ignore
    public void testListBuckets() {
        // 列举桶
        ListBucketsRequest request = new ListBucketsRequest();
        request.setQueryLocation(true);
        List<ObsBucket> buckets = client.listBuckets(request);
        for (ObsBucket bucket : buckets) {
            System.out.println(bucket.getBucketName() + ": " + bucket.getCreationDate());
        }
    }

    @Test
    @Ignore
    public void testPutObject() throws IOException {
        // 上传对象
        PutObjectResult result = client.putObject(bucketName, testFileName, new File(testFileDirectory, testFileName));
        Assert.assertNotNull(result.getRequestId());
    }

    @Test
    @Ignore
    public void testGetObject() throws IOException {
        // 下载对象
        String storageFileName = UUID.randomUUID().toString() + testFileName.substring(testFileName.lastIndexOf("."));
        ObsObject object = client.getObject(bucketName, testFileName);
        InputStream input = object.getObjectContent();
        FileOutputStream output = new FileOutputStream(testFileDirectory + storageFileName);
        IOUtils.copy(input, output);
        input.close();
        output.close();
    }

    @Test
    @Ignore
    public void testListObjects() {
        // 列举对象
        ObjectListing list = client.listObjects(bucketName);
        for (ObsObject object : list.getObjects()) {
            System.out.println(object.getObjectKey() + ": " + object.getMetadata().getContentLength());
        }
    }

    @Test
    @Ignore
    public void testDeleteObject() {
        // 删除对象
        client.deleteObject(bucketName, testFileName);
    }
}
