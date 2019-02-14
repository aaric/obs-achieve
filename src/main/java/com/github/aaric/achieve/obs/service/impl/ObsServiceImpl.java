package com.github.aaric.achieve.obs.service.impl;

import com.github.aaric.achieve.obs.service.ObsService;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObjectMetadata;
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
        ObsClient client = getClient();
        try {
            // 如果返回对象不为null，说明该存在该远程文件
            ObjectMetadata metadata = client.getObjectMetadata(bucketName, remotePath);
            if(null != metadata) {
                return true;
            }

        } catch (ObsException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean uploadFile(String remotePath, File uploadFile) {
        return false;
    }

    @Override
    public void uploadFiles(Map<String, File> mapUploadFiles) {

    }

    @Override
    public File downloadFile(String remoteFilePath, String localFileDirectory, String localFileName) {
        return null;
    }
}
