package com.github.aaric.achieve.obs.service.impl;

import com.github.aaric.achieve.obs.service.ObsService;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.PutObjectResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
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
                return true;
            }

        } catch (ObsException e) {
            e.printStackTrace();
        }
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
        logger.info("## uploadFile: {}", remotePath);
        request.setProgressListener((progressStatus) -> {
            // 获得上传平均速率
            logger.info("AverageSpeed: {}", progressStatus.getAverageSpeed());
            // 获得上传进度百分比
            logger.info("TransferPercentage: ", progressStatus.getTransferPercentage());
        });
        request.setProgressInterval(1024 * 1024L);
        PutObjectResult result = getClient().putObject(request);
        if(null != result && StringUtils.isNotBlank(result.getRequestId())) {
            return redirectHost.join("/", remotePath);
        }
        return null;
    }

    @Override
    public Map<String, String> uploadFiles(Map<String, File> mapUploadFiles) {
        return null;
    }

    @Override
    public File downloadFile(String remoteFilePath, String localFileDirectory, String localFileName) {
        return null;
    }
}
