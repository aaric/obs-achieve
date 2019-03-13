package com.github.aaric.achieve.obs.service.impl;

import com.github.aaric.achieve.obs.service.ObsService;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * OBS文件服务接口实现
 *
 * @author Aaric, created on 2019-02-14T10:55.
 * @since 0.2.0-SNAPSHOT
 */
@Service
public class ObsServiceImpl implements ObsService {

    /**
     * Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(ObsServiceImpl.class);

    /**
     * 存储区域
     */
    @Value("${huawei.obs.endPoint}")
    private String endPoint;

    /**
     * 授权访问密钥（AK）
     */
    @Value("${huawei.obs.accessKey}")
    private String accessKey;

    /**
     * 授权访问密钥（SK）
     */
    @Value("${huawei.obs.accessSecretKey}")
    private String accessSecretKey;

    /**
     * 访问桶（Bucket ）
     */
    @Value("${huawei.obs.bucketName}")
    private String bucketName;

    /**
     * 企业重定向域名
     */
    @Value("${huawei.obs.redirectHost}")
    private String redirectHost;

    /**
     * 获得OBS客户端
     *
     * @return
     */
    private ObsClient getClient() {
        // 创建ObsClient实例
        return new ObsClient(accessKey, accessSecretKey, endPoint);
    }

    @Override
    public boolean isHasFile(String remotePath) {
        // 检查和格式化OBS指定存储目录
        if(StringUtils.isNotBlank(remotePath) && 0 == remotePath.indexOf("/")) {
            remotePath = remotePath.substring(1);
        }

        try {
            // 如果返回对象不为null，说明该存在该远程文件
            ObjectMetadata metadata = getClient().getObjectMetadata(bucketName, remotePath);
            if(null != metadata) {
                logger.info("## ObsService.isHasFile: {} true!", remotePath);
                return true;
            }

        } catch (ObsException e) {
            e.printStackTrace();
        }

        logger.info("## ObsService.isHasFile: {} false!", remotePath);
        return false;
    }

    @Override
    public String uploadFile(String remotePath, File uploadFile) {
        // 检查和格式化OBS指定存储目录
        if(StringUtils.isNotBlank(remotePath) && 0 == remotePath.indexOf("/")) {
            remotePath = remotePath.substring(1);
        }

        // 获取上传文件进度
        PutObjectRequest request = new PutObjectRequest(bucketName, remotePath);
        request.setFile(uploadFile);
        logger.info("## ObsService.uploadFile: {}", remotePath);
        request.setProgressListener((progressStatus) -> {
            // 获得上传平均速率
            logger.info("AverageSpeed: {}", progressStatus.getAverageSpeed());
            // 获得上传进度百分比
            logger.info("TransferPercentage: ", progressStatus.getTransferPercentage());
        });
        request.setProgressInterval(1024 * 1024L);
        PutObjectResult result = getClient().putObject(request);
        if(null != result && StringUtils.isNotBlank(result.getRequestId())) {
            return StringUtils.join(Arrays.asList(redirectHost, remotePath), "/");
        }
        return null;
    }

    @Override
    public Map<String, String> uploadFiles(Map<String, File> mapUploadFiles) {
        if(null != mapUploadFiles && 0 != mapUploadFiles.size()) {
            Map.Entry<String, File> entry;
            Map<String, String> mapStoragePaths = new HashMap<>();
            Iterator<Map.Entry<String, File>> it = mapUploadFiles.entrySet().iterator();
            while (it.hasNext()) {
                entry = it.next();
                mapStoragePaths.put(entry.getKey(), uploadFile(entry.getKey(), entry.getValue()));
            }
            return mapStoragePaths;
        }
        return null;
    }

    @Override
    public String downloadFile(String remotePath) {
        // 检查和格式化OBS指定存储目录
        if(StringUtils.isNotBlank(remotePath) && 0 == remotePath.indexOf("/")) {
            remotePath = remotePath.substring(1);
        }

        // 多线程下载文件
        DownloadFileRequest request = new DownloadFileRequest(bucketName, remotePath);
        String downloadFile = FileUtils.getTempDirectoryPath() + remotePath.substring(remotePath.lastIndexOf("/")+ 1);
        request.setDownloadFile(downloadFile);
        request.setTaskNum(5); //设置分段下载时的最大并发数
        request.setPartSize(10 * 1024 * 1024); //设置分段大小为10MB
        request.setEnableCheckpoint(true); //开启断点续传模式

        // 返回临时下载路径
        DownloadFileResult result = getClient().downloadFile(request);
        if(null != result) {
            logger.info("## ObsService.downloadFile: {} success!", remotePath);
            return downloadFile;
        }

        // 下载文件失败
        logger.info("## ObsService.downloadFile: {} failure!", remotePath);
        return null;
    }
}
