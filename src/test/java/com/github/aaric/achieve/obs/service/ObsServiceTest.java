package com.github.aaric.achieve.obs.service;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * ObsServiceTest
 *
 * @author Aaric, created on 2019-02-14T10:57.
 * @since 0.2.0-SNAPSHOT
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class ObsServiceTest {

    /**
     * 测试文件
     */
    private static final String testFileDirectory = FileUtils.getUserDirectoryPath() + "\\Desktop\\";
    private static final String testFileName = "banzhuan.jpg";

    @Autowired
    protected ObsService obsService;

    @Test
    @Ignore
    public void testIsHasFile() {
        Assert.assertTrue(obsService.isHasFile("/" + testFileName));
        Assert.assertFalse(obsService.isHasFile("/" + "404x.jpg"));
    }

    @Test
    @Ignore
    public void testUploadFile() {
        String storagePath = obsService.uploadFile("/hello/" + UUID.randomUUID().toString() + "/" + testFileName, new File(testFileDirectory, testFileName));
        System.out.println(storagePath);
        Assert.assertNotNull(storagePath);
    }

    @Test
    @Ignore
    public void testUploadFiles() {
        Map<String, File> mapUploadFiles = new HashMap<>();
        mapUploadFiles.put("/hello/" + UUID.randomUUID().toString() + "/" + testFileName, new File(testFileDirectory, testFileName));
        mapUploadFiles.put("/hello/" + UUID.randomUUID().toString() + "/" + testFileName, new File(testFileDirectory, testFileName));
        mapUploadFiles.put("/hello/" + UUID.randomUUID().toString() + "/" + testFileName, new File(testFileDirectory, testFileName));
        Map<String, String> storagePaths = obsService.uploadFiles(mapUploadFiles);
        storagePaths.forEach((key, value) -> {
            System.out.println(String.format("%s: %s", key, value));
        });
        Assert.assertNotEquals(0, storagePaths.size());
    }

    @Test
    @Ignore
    public void testDownloadFile() {
        String downloadPath = obsService.downloadFile("/hello/c96ed141-72ec-4d0d-a32a-5fe7ba075cad/banzhuan.jpg");
        System.out.println(downloadPath);
        Assert.assertNotNull(downloadPath);
    }
}
