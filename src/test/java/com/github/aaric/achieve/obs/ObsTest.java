package com.github.aaric.achieve.obs;

import com.obs.services.ObsClient;
import com.obs.services.model.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
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
    private static final String testFileDirectory = FileUtils.getUserDirectoryPath() + "\\Desktop\\";
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
    public void testGetBucketLocation() {
        String location = client.getBucketLocation(bucketName);
        System.out.println(location);
        Assert.assertEquals("cn-south-1", location);
    }

    @Test
    @Ignore
    public void testGetBucketStorageInfo() {
        BucketStorageInfo storageInfo = client.getBucketStorageInfo(bucketName);
        System.out.println(storageInfo.getObjectNumber());
        System.out.println(storageInfo.getSize());
        Assert.assertNotNull(storageInfo);
    }

    @Test
    @Ignore
    public void testPutObject() throws IOException {
        // 上传字符串
        //PutObjectResult result = client.putObject(bucketName, "uuid.txt", new ByteArrayInputStream(UUID.randomUUID().toString().getBytes()));

        // 上传网络流
        //PutObjectResult result = client.putObject(bucketName, "baidu.html", new URL("https://www.baidu.com").openStream());

        // 上传文件流
        //PutObjectResult result = client.putObject(bucketName, testFileName, new File(testFileDirectory, testFileName));

        // 获取上传文件进度
        PutObjectRequest request = new PutObjectRequest(bucketName, testFileName);
        request.setFile(new File(testFileDirectory, testFileName));
        request.setProgressListener((progressStatus) -> {
            // 获得上传平均速率
            System.out.println("AverageSpeed: " + progressStatus.getAverageSpeed());
            // 获得上传进度百分比
            System.out.println("TransferPercentage: " + progressStatus.getTransferPercentage());
        });
        request.setProgressInterval(1024 * 1024L);
        PutObjectResult result = client.putObject(request);
        Assert.assertNotNull(result.getRequestId());
    }

    @Test
    @Ignore
    public void testPutObjectDirectory() {
        // 创建文件夹
        PutObjectResult result = client.putObject(bucketName, "new_folder/", new ByteArrayInputStream(new byte[0]));
        Assert.assertNotNull(result.getRequestId());
    }

    @Test
    @Ignore
    public void testUploadFile() {
        // 断点续传上传 -- 待完善
        UploadFileRequest request = new UploadFileRequest(bucketName, testFileName);
        request.setUploadFile(testFileDirectory + testFileName);
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
    public void testDownloadFile() {
        // 多线程下载文件
        String storageFileName = UUID.randomUUID().toString() + testFileName.substring(testFileName.lastIndexOf("."));
        DownloadFileRequest request = new DownloadFileRequest(bucketName, testFileName);
        request.setDownloadFile(testFileDirectory + storageFileName);
        request.setTaskNum(5); //设置分段下载时的最大并发数
        request.setPartSize(2 * 1024); //设置分段大小为10MB：10 * 1024 * 1024
        request.setEnableCheckpoint(true); //开启断点续传模式
        DownloadFileResult result = client.downloadFile(request);
        Assert.assertNotNull(result);
    }

    @Test
    @Ignore
    public void testListObjects() {
        // 列举对象
        //ObjectListing list = client.listObjects(bucketName);
        ListObjectsRequest request = new ListObjectsRequest(bucketName);

        // 只列举100个对象(默认1000)
        request.setMaxKeys(2);

        // 设置文件夹对象名"new_folder/"为前缀
        //request.setPrefix("new_folder/");

        // 设置文件夹分隔符"/"
        //request.setDelimiter("/");

        // 分页列举全部对象
        ObjectListing result;
        do {
            result = client.listObjects(request);
            for (ObsObject object : result.getObjects()) {
                System.out.println(object.getOwner().getId() + ": " + object.getObjectKey() + ": " + object.getMetadata().getContentLength());
            }

            // 指定起始位置列举
            request.setMarker(result.getNextMarker());

        } while (result.isTruncated());
    }

    @Test
    @Ignore
    public void testDeleteObject() {
        // 删除对象
        client.deleteObject(bucketName, testFileName);
    }

    /**
     * 注意：会删除全部文件!!!
     */
    @Test
    @Ignore
    public void testDeleteObjectList() {
        // 批量删除对象
        ListVersionsRequest request = new ListVersionsRequest(bucketName);

        // 每次批量删除100个对象
        request.setMaxKeys(100);

        ListVersionsResult result;
        do {
            // 执行删除
            result = client.listVersions(request);

            DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucketName);
            for (VersionOrDeleteMarker v: result.getVersions()) {
                deleteRequest.addKeyAndVersion(v.getKey(), v.getVersionId());
            }

            DeleteObjectsResult deleteResult = client.deleteObjects(deleteRequest);
            System.out.println(deleteResult.getDeletedObjectResults());

            // 获取下一批次
            request.setKeyMarker(result.getNextKeyMarker());
            request.setVersionIdMarker(result.getNextVersionIdMarker());

        } while (result.isTruncated());
    }

    @Test
    @Ignore
    public void testPutWithSetACL() {
        // 上传时设置文件ACL属性
        String objectKey = "private" + testFileName.substring(testFileName.lastIndexOf("."));
        PutObjectRequest request = new PutObjectRequest(bucketName, objectKey);
        request.setFile(new File(testFileDirectory, testFileName));
        request.setAcl(AccessControlList.REST_CANNED_PRIVATE); //账号无权限，ALC设置无效
        PutObjectResult result = client.putObject(request);
        Assert.assertNotNull(result.getRequestId());
    }

    @Test
    @Ignore
    public void testSetACL() {
        // 设置对象访问权限为私有读写
        client.setObjectAcl(bucketName, testFileName, AccessControlList.REST_CANNED_PRIVATE); //账号无权限，ALC设置无效
    }

    @Test
    @Ignore
    public void testGetAccessControlList() {
        // 获取对象访问权限
        AccessControlList acl = client.getObjectAcl(bucketName, testFileName);
        System.out.println(acl);
        Assert.assertNotNull(acl);
    }

    @Test
    @Ignore
    public void testCopyObject() {
        // 复制对象
        CopyObjectRequest request = new CopyObjectRequest(bucketName, testFileName, bucketName, "copy-" + testFileName);
        CopyObjectResult result = client.copyObject(request);
        System.out.println(result);
        Assert.assertNotNull(result);
    }
}
